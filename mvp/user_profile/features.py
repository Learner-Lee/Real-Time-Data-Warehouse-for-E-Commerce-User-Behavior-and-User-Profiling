"""用户特征：从 DWS 用户 × 天表、MySQL 订单、用户维表，算出截止日期之前 N 天窗口内的用户特征。

防穿越：只用截止时间之前的数据。
- 行为：只取窗口内（截止日期之前的 N 天）的 DWS 行；
- 订单：只取窗口内下的单；退款状态按 update_time 判断，截止时间之后才退款的订单，在截止时刻仍算已支付；
- 标签（未来 N 天是否购买）单独存一列，不参与训练特征。
"""
from datetime import date, datetime, timedelta, timezone

import numpy as np
import pandas as pd

TZ = timezone(timedelta(hours=8))
LEVEL_CODE = {"bronze": 0, "silver": 1, "gold": 2, "platinum": 3}

# 特征清单：(列名, 中文说明)。顺序就是特征表的列顺序；说明会写进 Doris 列注释
FEATURES = [
    # 窗口内总量
    ("pv_cnt", "浏览次数"),
    ("cart_cnt", "加购次数"),
    ("fav_cnt", "收藏次数"),
    ("buy_cnt", "购买行为次数（日志）"),
    ("event_cnt", "全部行为次数"),
    ("item_cnt_sum", "每天接触的不同商品数之和"),
    ("category_cnt_sum", "每天接触的不同类目数之和"),
    ("buy_item_cnt", "每天购买的不同商品数之和"),
    ("active_hour_sum", "每天活跃小时数之和"),
    ("night_event_cnt", "0–6 点的行为次数"),
    # 活跃天数
    ("active_days", "活跃天数"),
    ("active_day_ratio", "活跃天数占窗口天数的比例"),
    ("buy_days", "有购买的天数"),
    ("cart_days", "有加购的天数"),
    ("fav_days", "有收藏的天数"),
    # 按活跃天平均
    ("pv_per_day", "活跃日平均浏览次数"),
    ("cart_per_day", "活跃日平均加购次数"),
    ("fav_per_day", "活跃日平均收藏次数"),
    ("buy_per_day", "活跃日平均购买次数"),
    ("event_per_day", "活跃日平均行为次数"),
    ("items_per_day", "活跃日平均接触商品数"),
    ("categories_per_day", "活跃日平均接触类目数"),
    ("hours_per_day", "活跃日平均活跃小时数"),
    # 单日峰值和波动
    ("max_daily_event", "单日最多行为次数"),
    ("max_daily_pv", "单日最多浏览次数"),
    ("max_daily_cart", "单日最多加购次数"),
    ("max_daily_buy", "单日最多购买次数"),
    ("std_daily_event", "每天行为次数的标准差（没活跃的天按 0 算）"),
    # 转化和偏好
    ("cart_rate", "加购率：加购 ÷ 浏览"),
    ("fav_rate", "收藏率：收藏 ÷ 浏览"),
    ("buy_rate", "购买率：购买 ÷ 浏览"),
    ("cart_to_buy_rate", "加购后购买率：购买 ÷ 加购"),
    ("engage_rate", "深度互动占比：（加购 + 收藏 + 购买）÷ 全部行为"),
    ("night_ratio", "夜间行为占比"),
    ("items_per_event", "每条行为对应的商品数（越高越分散）"),
    ("categories_per_item", "每个商品对应的类目数（越高兴趣越广）"),
    # 时间远近
    ("hours_since_last_event", "最后一条行为距截止时间的小时数"),
    ("hours_since_first_event", "窗口内第一条行为距截止时间的小时数"),
    ("hours_since_last_buy", "最后一次购买距截止时间的小时数，窗口内没买过记为窗口长度 + 24"),
    ("days_since_last_active", "最后活跃日距截止日期的天数"),
    # 最近一天
    ("last_day_event", "截止前最后一天的行为次数"),
    ("last_day_pv", "截止前最后一天的浏览次数"),
    ("last_day_cart", "截止前最后一天的加购次数"),
    ("last_day_buy", "截止前最后一天的购买次数"),
    ("last_day_share", "最后一天行为占窗口行为的比例"),
    ("trend_ratio", "最后一天行为 ÷ 之前几天日均行为（+1 平滑），大于 1 说明变活跃"),
    # 购买意向
    ("cart_not_buy", "加购多于购买的次数（加购 − 购买，最少为 0）"),
    ("has_cart_not_buy", "是否有加购未买（0/1）"),
    # 订单（MySQL）
    ("order_cnt", "窗口内下单、截止时仍为已支付的订单数"),
    ("order_amount", "这些订单的金额合计（元，模拟价格）"),
    ("avg_order_amount", "平均订单金额（元）"),
    ("max_order_amount", "最大一笔订单金额（元）"),
    ("refund_cnt", "截止时已退款的订单数"),
    ("refund_ratio", "退款订单占窗口内下单数的比例"),
    # 维度
    ("user_level_code", "用户等级编码：bronze 0 / silver 1 / gold 2 / platinum 3（模拟）"),
]
FEATURE_NAMES = [name for name, _ in FEATURES]


def day_start_ms(d: date) -> int:
    """北京时间某天 0 点的毫秒时间戳。"""
    return int(datetime(d.year, d.month, d.day, tzinfo=TZ).timestamp() * 1000)


def build_features(dws: pd.DataFrame, orders: pd.DataFrame, dim_user: pd.DataFrame,
                   cutoff: date, window_days: int) -> pd.DataFrame:
    """截止日期 cutoff（不含当天）之前 window_days 天的特征。人群：窗口内有行为的用户。"""
    start = cutoff - timedelta(days=window_days)
    w = dws[(dws.dt >= start) & (dws.dt < cutoff)]
    cutoff_ms = day_start_ms(cutoff)
    g = w.groupby("user_id")
    f = pd.DataFrame({
        "pv_cnt": g.pv_cnt.sum(), "cart_cnt": g.cart_cnt.sum(), "fav_cnt": g.fav_cnt.sum(),
        "buy_cnt": g.buy_cnt.sum(), "event_cnt": g.event_cnt.sum(),
        "item_cnt_sum": g.item_cnt.sum(), "category_cnt_sum": g.category_cnt.sum(),
        "buy_item_cnt": g.buy_item_cnt.sum(), "active_hour_sum": g.active_hour_cnt.sum(),
        "night_event_cnt": g.night_event_cnt.sum(),
        "active_days": g.dt.nunique(),
        "buy_days": g.buy_cnt.apply(lambda s: int((s > 0).sum())),
        "cart_days": g.cart_cnt.apply(lambda s: int((s > 0).sum())),
        "fav_days": g.fav_cnt.apply(lambda s: int((s > 0).sum())),
        "max_daily_event": g.event_cnt.max(), "max_daily_pv": g.pv_cnt.max(),
        "max_daily_cart": g.cart_cnt.max(), "max_daily_buy": g.buy_cnt.max(),
        "last_event_ts": g.last_event_ts.max(), "first_event_ts": g.first_event_ts.min(),
        "last_buy_ts": g.last_buy_ts.max(), "last_active_dt": g.dt.max(),
    })
    days = f.active_days
    f["active_day_ratio"] = days / window_days
    for col, src in [("pv_per_day", "pv_cnt"), ("cart_per_day", "cart_cnt"), ("fav_per_day", "fav_cnt"),
                     ("buy_per_day", "buy_cnt"), ("event_per_day", "event_cnt"), ("items_per_day", "item_cnt_sum"),
                     ("categories_per_day", "category_cnt_sum"), ("hours_per_day", "active_hour_sum")]:
        f[col] = f[src] / days
    # 每天行为次数的标准差：没活跃的天补 0
    daily = w.pivot_table(index="user_id", columns="dt", values="event_cnt", aggfunc="sum", fill_value=0)
    daily = daily.reindex(columns=[start + timedelta(days=i) for i in range(window_days)], fill_value=0)
    f["std_daily_event"] = daily.std(axis=1, ddof=0)

    safe = lambda a, b: np.where(b > 0, a / np.where(b > 0, b, 1), 0.0)
    f["cart_rate"] = safe(f.cart_cnt, f.pv_cnt)
    f["fav_rate"] = safe(f.fav_cnt, f.pv_cnt)
    f["buy_rate"] = safe(f.buy_cnt, f.pv_cnt)
    f["cart_to_buy_rate"] = safe(f.buy_cnt, f.cart_cnt)
    f["engage_rate"] = safe(f.cart_cnt + f.fav_cnt + f.buy_cnt, f.event_cnt)
    f["night_ratio"] = safe(f.night_event_cnt, f.event_cnt)
    f["items_per_event"] = safe(f.item_cnt_sum, f.event_cnt)
    f["categories_per_item"] = safe(f.category_cnt_sum, f.item_cnt_sum)

    hour = 3600 * 1000
    f["hours_since_last_event"] = (cutoff_ms - f.last_event_ts) / hour
    f["hours_since_first_event"] = (cutoff_ms - f.first_event_ts) / hour
    f["hours_since_last_buy"] = ((cutoff_ms - f.last_buy_ts) / hour).fillna(window_days * 24 + 24)
    f["days_since_last_active"] = f.last_active_dt.map(lambda d: (cutoff - d).days)

    last_day = cutoff - timedelta(days=1)
    ld = w[w.dt == last_day].set_index("user_id")
    for col, src in [("last_day_event", "event_cnt"), ("last_day_pv", "pv_cnt"),
                     ("last_day_cart", "cart_cnt"), ("last_day_buy", "buy_cnt")]:
        f[col] = ld[src].reindex(f.index).fillna(0)
    f["last_day_share"] = safe(f.last_day_event, f.event_cnt)
    earlier_avg = (f.event_cnt - f.last_day_event) / max(window_days - 1, 1)
    f["trend_ratio"] = (f.last_day_event + 1) / (earlier_avg + 1)
    f["cart_not_buy"] = (f.cart_cnt - f.buy_cnt).clip(lower=0)
    f["has_cart_not_buy"] = (f.cart_not_buy > 0).astype(int)

    # 订单：窗口内下的单；截止时刻的状态（截止之后才退款的仍算已支付）
    start_dt = datetime(start.year, start.month, start.day)
    cutoff_dt = datetime(cutoff.year, cutoff.month, cutoff.day)
    o = orders[(orders.create_time >= start_dt) & (orders.create_time < cutoff_dt)].copy()
    o["refunded"] = (o.status == "REFUNDED") & (o.update_time < cutoff_dt)
    paid = o[~o.refunded].groupby("user_id").amount
    f["order_cnt"] = paid.count().reindex(f.index).fillna(0)
    f["order_amount"] = paid.sum().reindex(f.index).fillna(0)
    f["avg_order_amount"] = paid.mean().reindex(f.index).fillna(0)
    f["max_order_amount"] = paid.max().reindex(f.index).fillna(0)
    f["refund_cnt"] = o[o.refunded].groupby("user_id").size().reindex(f.index).fillna(0)
    placed = o.groupby("user_id").size().reindex(f.index).fillna(0)
    f["refund_ratio"] = safe(f.refund_cnt, placed)

    f["user_level_code"] = dim_user.set_index("user_id").level.map(LEVEL_CODE).reindex(f.index).fillna(-1)
    out = f[FEATURE_NAMES].astype(float)
    out.index.name = "user_id"
    return out


def build_label(dws: pd.DataFrame, cutoff: date, horizon_days: int) -> pd.Series:
    """截止日期之后 horizon_days 天内是否有购买（0/1），按用户。"""
    end = cutoff + timedelta(days=horizon_days)
    fut = dws[(dws.dt >= cutoff) & (dws.dt < end)]
    return (fut.groupby("user_id").buy_cnt.sum() > 0).astype(int)


def feature_table_ddl(table: str) -> str:
    cols = ",\n".join(f"  `{name}` DOUBLE COMMENT '{desc}'" for name, desc in FEATURES)
    return f"""CREATE TABLE {table} (
  cutoff_dt DATE COMMENT '截止日期（不含当天）：特征只用这天之前的数据',
  user_id BIGINT,
  window_days INT COMMENT '特征窗口天数',
{cols},
  label_buy_3d TINYINT COMMENT '标签（不是特征）：截止日期后 3 天是否购买，未知为空'
)
UNIQUE KEY (cutoff_dt, user_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 3
PROPERTIES ("replication_num" = "1")"""
