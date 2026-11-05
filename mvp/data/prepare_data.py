"""把 UserBehavior.csv（真实数据或模拟数据）处理成回放文件和维表。

1. 只保留数据集声明的时间范围（2017-11-25 至 2017-12-03，北京时间），去掉时间异常的行和未知行为
2. 按 user_id 抽样（默认 1%，真实数据约 100 万行），让虚拟机跑得动；抽样按用户整体抽，保证用户行为序列完整
3. 按时间戳排序，写出回放文件 behavior_sorted.csv
4. 生成维表 dim_user.csv（等级、城市）和 dim_item.csv（类目、价格）。
   属性由 id 的哈希决定，同样的输入每次生成的结果都一样

用 DuckDB 处理：真实数据 3.5GB 也不用一次性读进内存。
"""
import argparse
import os

import duckdb

START_TS = 1511539200  # 2017-11-25 00:00:00 +08:00
END_TS = 1512316799    # 2017-12-03 23:59:59 +08:00
CITIES = ["北京", "上海", "广州", "深圳", "杭州", "成都", "武汉", "西安", "南京", "重庆"]


def sql_str(value):
    """把字符串写成 SQL 字面量（单引号转义）。"""
    return "'" + value.replace("'", "''") + "'"


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", required=True, help="UserBehavior.csv 的路径（真实或模拟数据）")
    parser.add_argument("--out-dir", default="/data/r1", help="输出目录，默认 /data/r1")
    parser.add_argument("--sample-mod", type=int, default=100,
                        help="只保留 user_id %% N == 0 的用户：1 表示全量；默认 100，约 1%%")
    args = parser.parse_args()
    os.makedirs(args.out_dir, exist_ok=True)
    out = args.out_dir.rstrip("/")

    con = duckdb.connect()
    con.execute(f"""
        CREATE TABLE events AS
        SELECT *
        FROM read_csv({sql_str(args.input)}, header = false, columns = {{
            'user_id': 'BIGINT', 'item_id': 'BIGINT', 'category_id': 'BIGINT', 'behavior': 'VARCHAR', 'ts': 'BIGINT'
        }})
        WHERE ts BETWEEN {START_TS} AND {END_TS}
          AND behavior IN ('pv', 'cart', 'fav', 'buy')
          AND user_id % {args.sample_mod} = 0
    """)

    # 回放文件：按时间排序（同一秒内按用户排序，保证每次结果一样）
    con.execute(f"""
        COPY (SELECT user_id, item_id, category_id, behavior, ts FROM events ORDER BY ts, user_id, item_id)
        TO {sql_str(out + '/behavior_sorted.csv')} (HEADER false)
    """)

    # 用户维表：等级按 50% / 30% / 15% / 5% 分布，城市 10 选 1
    cities = "[" + ", ".join(sql_str(c) for c in CITIES) + "]"
    con.execute(f"""
        COPY (
            SELECT user_id,
                   CASE WHEN h < 50 THEN 'bronze' WHEN h < 80 THEN 'silver' WHEN h < 95 THEN 'gold' ELSE 'platinum' END AS level,
                   ({cities})[CAST(h % 10 AS BIGINT) + 1] AS city
            FROM (SELECT DISTINCT user_id, hash(user_id) % 100 AS h FROM events)
            ORDER BY user_id
        ) TO {sql_str(out + '/dim_user.csv')} (HEADER false)
    """)

    # 商品维表：类目取数据里的值；价格按对数均匀分布在约 4.5 到 245 元之间（便宜的商品多、贵的少）
    con.execute(f"""
        COPY (
            SELECT item_id,
                   ANY_VALUE(category_id) AS category_id,
                   CAST(ROUND(EXP(1.5 + 4.0 * (hash(item_id) % 10000) / 10000.0), 2) AS DECIMAL(10, 2)) AS price
            FROM events
            GROUP BY item_id
            ORDER BY item_id
        ) TO {sql_str(out + '/dim_item.csv')} (HEADER false)
    """)

    rows, users, items, first_ts, last_ts = con.execute(
        "SELECT COUNT(*), COUNT(DISTINCT user_id), COUNT(DISTINCT item_id), MIN(ts), MAX(ts) FROM events").fetchone()
    print(f"回放文件：{rows:,} 行，{users:,} 个用户，{items:,} 个商品")
    print(f"时间范围：{first_ts} ～ {last_ts}（秒）")
    for behavior, cnt in con.execute(
            "SELECT behavior, COUNT(*) FROM events GROUP BY behavior ORDER BY COUNT(*) DESC").fetchall():
        print(f"  {behavior:<4} {cnt:>10,}  {cnt / rows:6.2%}")
    print(f"输出目录：{out}（behavior_sorted.csv、dim_user.csv、dim_item.csv）")


if __name__ == "__main__":
    main()
