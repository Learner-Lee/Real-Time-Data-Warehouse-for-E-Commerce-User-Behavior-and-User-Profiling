"""R1 用户画像：特征 → RFM + K-Means 分群 → XGBoost 未来 3 天购买（复购）预测 → 标签写回 Doris → 报告。

由 bin/profile.sh 调用（它先建表、从 ODS 生成 dws_user_behavior_1d）。可以重复运行，结果覆盖写入。
MySQL 业务库密码从环境变量 MYSQL_APP_PASSWORD 读取。
"""
import argparse
import json
import os
import sys
from datetime import date, datetime, timedelta

import numpy as np
import pandas as pd
import pymysql
from sklearn.cluster import KMeans
from sklearn.metrics import average_precision_score, roc_auc_score, silhouette_score
from sklearn.preprocessing import StandardScaler
from xgboost import XGBClassifier

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from features import FEATURES, FEATURE_NAMES, build_features, build_label, feature_table_ddl  # noqa: E402

WINDOW = 3                       # 特征窗口和预测窗口都是 3 天（数据只有 9 天）
TRAIN_CUTOFF = date(2017, 11, 28)   # 特征 11-25～11-27，标签 11-28～11-30
TEST_CUTOFF = date(2017, 12, 1)     # 特征 11-28～11-30，标签 12-01～12-03（时间上在训练集之后）
SCORE_CUTOFF = date(2017, 12, 4)    # 特征 12-01～12-03，预测数据之外的未来 3 天
RFM_CLASS = {  # (R 高, F 高, M 高) → 经典 RFM 八类；R 高 = 最近买过
    (1, 1, 1): "重要价值客户", (1, 0, 1): "重要发展客户", (0, 1, 1): "重要保持客户", (0, 0, 1): "重要挽留客户",
    (1, 1, 0): "一般价值客户", (1, 0, 0): "一般发展客户", (0, 1, 0): "一般保持客户", (0, 0, 0): "一般挽留客户",
}


def log(msg):
    print(f"[{datetime.now():%H:%M:%S}] {msg}", flush=True)


def doris_conn(host):
    return pymysql.connect(host=host, port=9030, user="root", password="", database="r1_dw", autocommit=True)


def query(conn, sql):
    return pd.read_sql(sql, conn)


def insert_rows(conn, table, columns, rows, batch=1000):
    """分批 INSERT；NaN 写成 NULL。"""
    cols = ", ".join(f"`{c}`" for c in columns)
    marks = ", ".join(["%s"] * len(columns))
    sql = f"INSERT INTO {table} ({cols}) VALUES ({marks})"
    clean = [[None if (isinstance(v, float) and np.isnan(v)) else v for v in r] for r in rows]
    with conn.cursor() as cur:
        for i in range(0, len(clean), batch):
            cur.executemany(sql, clean[i:i + batch])


def score_1_5(values: pd.Series, higher_is_better=True) -> pd.Series:
    """按排名百分位打 1–5 分（并列取平均排名）。"""
    pct = values.rank(pct=True, method="average", ascending=higher_is_better)
    return np.ceil(pct * 5).clip(1, 5).astype(int)


def top_k_precision(y, p, frac):
    k = max(1, int(len(p) * frac))
    idx = np.argsort(-p)[:k]
    return float(np.asarray(y)[idx].mean())


def main():
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--doris-host", default="bigdata01")
    ap.add_argument("--mysql-host", default="bigdata01")
    ap.add_argument("--mysql-user", default="r1")
    ap.add_argument("--dim-user", required=True, help="dim_user.csv（user_id,level,city）")
    ap.add_argument("--out", required=True, help="报告、模型和指标的输出目录")
    args = ap.parse_args()
    os.makedirs(args.out, exist_ok=True)
    version = datetime.now().strftime("%Y%m%d%H%M%S")

    # ---------- 读数据 ----------
    doris = doris_conn(args.doris_host)
    dws = query(doris, "SELECT * FROM dws_user_behavior_1d")
    dws["dt"] = pd.to_datetime(dws.dt).dt.date
    my = pymysql.connect(host=args.mysql_host, user=args.mysql_user,
                         password=os.environ["MYSQL_APP_PASSWORD"], database="r1_shop")
    orders = query(my, "SELECT order_id, user_id, amount, status, create_time, update_time FROM orders")
    my.close()
    orders["amount"] = orders.amount.astype(float)
    dim_user = pd.read_csv(args.dim_user, header=None, names=["user_id", "level", "city"])
    log(f"读入 DWS 用户 × 天 {len(dws):,} 行（{dws.user_id.nunique():,} 个用户），订单 {len(orders):,} 单")

    # ---------- 特征（三个截止日期）----------
    feats = {c: build_features(dws, orders, dim_user, c, WINDOW) for c in (TRAIN_CUTOFF, TEST_CUTOFF, SCORE_CUTOFF)}
    labels = {c: build_label(dws, c, WINDOW) for c in (TRAIN_CUTOFF, TEST_CUTOFF)}
    with doris.cursor() as cur:
        cur.execute("DROP TABLE IF EXISTS dws_user_feature")
        cur.execute(feature_table_ddl("dws_user_feature"))
    for c, f in feats.items():
        y = labels.get(c)
        lab = y.reindex(f.index).fillna(0).astype(int) if y is not None else pd.Series(np.nan, index=f.index)
        rows = [[c, int(uid), WINDOW, *map(float, f.loc[uid].values), (None if pd.isna(lab[uid]) else int(lab[uid]))]
                for uid in f.index]
        insert_rows(doris, "dws_user_feature", ["cutoff_dt", "user_id", "window_days", *FEATURE_NAMES, "label_buy_3d"], rows)
    log(f"特征表 dws_user_feature：{len(FEATURES)} 个特征，" +
        "，".join(f"{c} 截止 {len(f):,} 人" for c, f in feats.items()))

    # ---------- XGBoost：未来 3 天是否购买 ----------
    Xtr, Xte = feats[TRAIN_CUTOFF], feats[TEST_CUTOFF]
    ytr = labels[TRAIN_CUTOFF].reindex(Xtr.index).fillna(0).astype(int)
    yte = labels[TEST_CUTOFF].reindex(Xte.index).fillna(0).astype(int)
    # 调参只用训练集：随机留 30% 用户做验证，测试集不参与挑选
    rng = np.random.default_rng(42)
    is_val = rng.random(len(Xtr)) < 0.3
    base = dict(learning_rate=0.05, subsample=0.8, colsample_bytree=0.8, reg_lambda=1.0,
                eval_metric="auc", random_state=42, n_jobs=2)
    spw = lambda y: (len(y) - y.sum()) / max(y.sum(), 1)
    grid = []
    for depth in (2, 3, 4):
        for mcw in (5, 20):
            for n_est in (100, 300):
                cand = dict(base, max_depth=depth, min_child_weight=mcw, n_estimators=n_est)
                mdl = XGBClassifier(scale_pos_weight=spw(ytr[~is_val]), **cand).fit(Xtr[~is_val], ytr[~is_val])
                auc = roc_auc_score(ytr[is_val], mdl.predict_proba(Xtr[is_val])[:, 1])
                grid.append({"max_depth": depth, "min_child_weight": mcw, "n_estimators": n_est, "val_auc": float(auc)})
    best = max(grid, key=lambda g: g["val_auc"])
    params = dict(base, max_depth=best["max_depth"], min_child_weight=best["min_child_weight"],
                  n_estimators=best["n_estimators"])
    log(f"调参（训练集内 30% 验证）：最好的是 max_depth={best['max_depth']}、min_child_weight={best['min_child_weight']}、"
        f"n_estimators={best['n_estimators']}，验证 AUC {best['val_auc']:.4f}")
    model = XGBClassifier(scale_pos_weight=spw(ytr), **params)
    model.fit(Xtr, ytr)
    p_te = model.predict_proba(Xte)[:, 1]
    p_tr = model.predict_proba(Xtr)[:, 1]
    buyers_te = (Xte.buy_cnt > 0).values  # 窗口内买过的人：严格意义上的「复购」
    m = {
        "train": {"cutoff": str(TRAIN_CUTOFF), "users": len(ytr), "positives": int(ytr.sum()), "pos_rate": float(ytr.mean()),
                  "auc": float(roc_auc_score(ytr, p_tr))},
        "test": {"cutoff": str(TEST_CUTOFF), "users": len(yte), "positives": int(yte.sum()), "pos_rate": float(yte.mean()),
                 "auc": float(roc_auc_score(yte, p_te)), "pr_auc": float(average_precision_score(yte, p_te)),
                 "precision_top10pct": top_k_precision(yte, p_te, 0.10)},
        "baseline_event_cnt_auc": float(roc_auc_score(yte, Xte.event_cnt)),
        "baseline_buy_cnt_auc": float(roc_auc_score(yte, Xte.buy_cnt)),
        "repurchase": {"users": int(buyers_te.sum()), "positives": int(yte[buyers_te].sum()),
                       "auc": float(roc_auc_score(yte[buyers_te], p_te[buyers_te]))
                       if 0 < yte[buyers_te].sum() < buyers_te.sum() else None,
                       "baseline_event_cnt_auc": float(roc_auc_score(yte[buyers_te], Xte.event_cnt[buyers_te]))
                       if 0 < yte[buyers_te].sum() < buyers_te.sum() else None},
    }
    imp = pd.Series(model.get_booster().get_score(importance_type="gain")).sort_values(ascending=False)
    m["top_features"] = [[k, round(float(v), 2)] for k, v in imp.head(10).items()]
    log(f"XGBoost 测试集（时间外）AUC {m['test']['auc']:.4f}，PR-AUC {m['test']['pr_auc']:.4f}；"
        f"只用行为次数的基线 AUC {m['baseline_event_cnt_auc']:.4f}；复购子集 AUC "
        f"{'—' if m['repurchase']['auc'] is None else format(m['repurchase']['auc'], '.4f')}")
    m["tuning"] = {"chosen": best, "grid": grid}

    # 用训练集 + 测试集重训，给 12-01～12-03 活跃的用户打分
    final = XGBClassifier(scale_pos_weight=spw(pd.concat([ytr, yte])), **params)
    final.fit(pd.concat([Xtr, Xte]), pd.concat([ytr, yte]))
    final.save_model(os.path.join(args.out, "xgb_buy_3d.json"))
    p_now = pd.Series(final.predict_proba(feats[SCORE_CUTOFF])[:, 1], index=feats[SCORE_CUTOFF].index)
    rank = p_now.rank(pct=True, ascending=False)
    level_now = pd.Series(np.where(rank <= 0.10, "高", np.where(rank <= 0.30, "中", "低")), index=p_now.index)

    # ---------- RFM（全部 9 天，截止 12-04）----------
    end_dt = datetime(SCORE_CUTOFF.year, SCORE_CUTOFF.month, SCORE_CUTOFF.day)
    paid = orders[orders.status == "PAID"]
    rfm = paid.groupby("user_id").agg(last=("create_time", "max"), f=("order_id", "count"), m=("amount", "sum"))
    rfm["r"] = (end_dt - rfm["last"]).dt.total_seconds() / 86400
    rfm["r_score"] = score_1_5(rfm.r, higher_is_better=False)
    rfm["f_score"] = score_1_5(rfm.f)
    rfm["m_score"] = score_1_5(rfm.m)
    hi = pd.DataFrame({"r": (rfm.r <= rfm.r.mean()).astype(int), "f": (rfm.f > rfm.f.mean()).astype(int),
                       "m": (rfm.m > rfm.m.mean()).astype(int)})
    rfm["rfm_class"] = [RFM_CLASS[(a, b, c)] for a, b, c in hi.itertuples(index=False)]

    # ---------- K-Means：对买过的用户，标准化后的 log(R)、log(F)、log(M) ----------
    Z = StandardScaler().fit_transform(np.log1p(rfm[["r", "f", "m"]]))
    k_table = []
    for k in range(2, 11):
        km = KMeans(n_clusters=k, n_init=10, random_state=42).fit(Z)
        k_table.append({"k": k, "inertia": float(km.inertia_), "silhouette": float(silhouette_score(Z, km.labels_))})
    candidates = [t for t in k_table if 3 <= t["k"] <= 8]
    best_k = max(candidates, key=lambda t: t["silhouette"])["k"]
    km = KMeans(n_clusters=best_k, n_init=10, random_state=42).fit(Z)
    rfm["cluster"] = km.labels_
    centers = rfm.groupby("cluster")[["r", "f", "m"]].mean().sort_values("m", ascending=False)
    order = {c: i for i, c in enumerate(centers.index)}  # 按平均金额从高到低重新编号
    rfm["segment_id"] = rfm.cluster.map(order)
    centers.index = [order[c] for c in centers.index]
    # 命名：在聚类用的标准化空间（log 后标准化）里看每个簇中心：
    # 高于平均 1.5 个标准差算「超高」，0.5 个算「高」，低于 0.5 个算「低」，中间算「中」
    zc = pd.DataFrame(km.cluster_centers_, columns=["r", "f", "m"]).rename(index=order)
    lvl = lambda z, top, hi, mid, lo: top if z > 1.5 else (hi if z > 0.5 else (lo if z < -0.5 else mid))
    names = {}
    for sid in sorted(zc.index):
        z = zc.loc[sid]
        name = (lvl(-z.r, "近期", "近期", "中期", "沉睡") + lvl(z.f, "超高频", "高频", "中频", "低频")
                + lvl(z.m, "超高额", "高额", "中额", "低额"))
        names[sid] = name if name not in names.values() else f"{name}（{sid}）"
    rfm["segment_name"] = rfm.segment_id.map(names)
    seg = rfm.groupby(["segment_id", "segment_name"]).agg(users=("f", "size"), r_days=("r", "mean"),
                                                          f_orders=("f", "mean"), m_amount=("m", "mean")).reset_index()
    log(f"K-Means：K = {best_k}（3–8 里轮廓系数最高），{len(rfm):,} 个买过的用户")

    # ---------- 标签写回 Doris ----------
    users = dws.groupby("user_id").agg(active_days=("dt", "nunique"), event_cnt=("event_cnt", "sum"))
    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    rows = []
    for uid, u in users.iterrows():
        r = rfm.loc[uid] if uid in rfm.index else None
        prob = float(p_now[uid]) if uid in p_now.index else None
        rows.append([int(uid), SCORE_CUTOFF, int(u.active_days), int(u.event_cnt),
                     None if r is None else round(float(r.r), 3), 0 if r is None else int(r.f),
                     0.0 if r is None else round(float(r.m), 2),
                     None if r is None else int(r.r_score), None if r is None else int(r.f_score),
                     None if r is None else int(r.m_score),
                     "未购买访客" if r is None else r.rfm_class,
                     -1 if r is None else int(r.segment_id), "未购买访客" if r is None else r.segment_name,
                     None if prob is None else round(prob, 6),
                     "近 3 天未活跃" if prob is None else level_now[uid], version, now])
    with doris.cursor() as cur:
        cur.execute("TRUNCATE TABLE ads_user_profile_tag")
    insert_rows(doris, "ads_user_profile_tag",
                ["user_id", "profile_dt", "active_days", "event_cnt", "r_days", "f_orders", "m_amount", "r_score",
                 "f_score", "m_score", "rfm_class", "segment_id", "segment_name", "buy_prob_3d", "buy_level_3d",
                 "model_version", "updated_at"], rows)
    check = query(doris, "SELECT COUNT(*) n, SUM(segment_id >= 0) buyers, SUM(buy_prob_3d IS NOT NULL) scored FROM ads_user_profile_tag")
    log(f"标签表 ads_user_profile_tag：{int(check.n[0]):,} 人，其中买过的 {int(check.buyers[0]):,} 人、有购买概率的 {int(check.scored[0]):,} 人")

    # ---------- 报告 ----------
    m.update({"version": version, "features": len(FEATURES), "best_k": best_k, "k_table": k_table,
              "segments": seg.to_dict("records"), "rfm_classes": rfm.rfm_class.value_counts().to_dict(),
              "buyers": len(rfm), "tag_users": int(check.n[0])})
    with open(os.path.join(args.out, "metrics.json"), "w") as fh:
        json.dump(m, fh, ensure_ascii=False, indent=2, default=str)
    write_report(os.path.join(args.out, "report.md"), m)
    log(f"报告：{args.out}/report.md")


def write_report(path, m):
    t, rp = m["test"], m["repurchase"]
    lines = [
        f"# R1 用户画像运行报告（模型版本 {m['version']}）", "",
        "> 数据是模拟的：购买与否按固定概率随机产生，只和活跃度有关。这里的数字只说明链路跑通，不代表真实复购规律。", "",
        "## 复购模型（XGBoost，未来 3 天是否购买）", "",
        "| 项 | 训练集 | 测试集（时间在训练集之后） |", "|---|---|---|",
        f"| 截止日期 | {m['train']['cutoff']} | {t['cutoff']} |",
        f"| 人数（窗口内活跃） | {m['train']['users']:,} | {t['users']:,} |",
        f"| 正样本（后 3 天买了） | {m['train']['positives']:,}（{m['train']['pos_rate']:.1%}） | {t['positives']:,}（{t['pos_rate']:.1%}） |",
        f"| AUC | {m['train']['auc']:.4f} | **{t['auc']:.4f}** |",
        f"| 参数（训练集内验证选出） | max_depth={m['tuning']['chosen']['max_depth']}，min_child_weight={m['tuning']['chosen']['min_child_weight']}，"
        f"n_estimators={m['tuning']['chosen']['n_estimators']}（验证 AUC {m['tuning']['chosen']['val_auc']:.4f}） | — |",
        f"| PR-AUC | — | {t['pr_auc']:.4f} |",
        f"| 预测概率最高的 10% 里真正购买的比例 | — | {t['precision_top10pct']:.1%} |", "",
        f"- 只用「行为次数」一个特征的基线 AUC：**{m['baseline_event_cnt_auc']:.4f}**；只用「购买次数」：{m['baseline_buy_cnt_auc']:.4f}。",
        f"- 窗口内买过的 {rp['users']:,} 人里（严格意义的复购，正样本 {rp['positives']:,}）：模型 AUC {rp['auc'] if rp['auc'] is None else format(rp['auc'], '.4f')}，"
        f"行为次数基线 {rp['baseline_event_cnt_auc'] if rp['baseline_event_cnt_auc'] is None else format(rp['baseline_event_cnt_auc'], '.4f')}。",
        "- 重要性最高的特征（gain）：" + "、".join(f"`{k}`" for k, _ in m["top_features"][:6]) + "。", "",
        f"## 分群（RFM + K-Means，{m['buyers']:,} 个买过的用户）", "",
        "| K | 簇内平方和 | 轮廓系数 |", "|---|---|---|",
        *[f"| {r['k']} | {r['inertia']:.1f} | {r['silhouette']:.4f} |" for r in m["k_table"]], "",
        f"选 K = **{m['best_k']}**（3–8 里轮廓系数最高）。", "",
        "| 编号 | 名称 | 人数 | 平均 R（天） | 平均 F（单） | 平均 M（元） |", "|---|---|---|---|---|---|",
        *[f"| {s['segment_id']} | {s['segment_name']} | {s['users']:,} | {s['r_days']:.2f} | {s['f_orders']:.2f} | {s['m_amount']:.2f} |"
          for s in m["segments"]], "",
        "经典 RFM 八类：" + "，".join(f"{k} {v:,}" for k, v in m["rfm_classes"].items()) + "。", "",
        f"## 标签", "", f"`ads_user_profile_tag` 共 {m['tag_users']:,} 人；特征表 `dws_user_feature` 有 {m['features']} 个特征。",
    ]
    with open(path, "w") as fh:
        fh.write("\n".join(lines) + "\n")


if __name__ == "__main__":
    main()
