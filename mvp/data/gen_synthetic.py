"""生成和阿里天池 UserBehavior.csv 同格式的模拟数据。还没下载真实数据集时，用它跑通整条链路。

格式（无表头）：user_id,item_id,category_id,behavior,timestamp（秒）
行为占比参照真实数据集：pv 约 89.5%、cart 约 5.5%、fav 约 2.9%、buy 约 2.1%
时间范围也和真实数据集一致：2017-11-25 00:00:00 至 2017-12-03 23:59:59（北京时间）
"""
import argparse

import numpy as np

START_TS = 1511539200  # 2017-11-25 00:00:00 +08:00
DAYS = 9
BEHAVIORS = np.array(["pv", "cart", "fav", "buy"])
BEHAVIOR_P = [0.895, 0.055, 0.029, 0.021]
# 一天 24 小时的流量权重：凌晨低、晚上高
HOUR_WEIGHTS = np.array([3, 2, 1, 1, 1, 1, 2, 3, 4, 5, 6, 6, 6, 6, 6, 6, 6, 7, 8, 9, 10, 10, 8, 5], dtype=float)


def main():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--out", required=True, help="输出的 CSV 路径")
    parser.add_argument("--rows", type=int, default=500_000, help="生成多少行，默认 50 万")
    parser.add_argument("--users", type=int, default=20_000, help="用户数")
    parser.add_argument("--items", type=int, default=100_000, help="商品数")
    parser.add_argument("--categories", type=int, default=500, help="类目数")
    parser.add_argument("--seed", type=int, default=42, help="随机种子，相同种子生成的数据相同")
    args = parser.parse_args()

    rng = np.random.default_rng(args.seed)
    n = args.rows
    # 少数活跃用户、热门商品贡献大部分行为（幂律分布），更接近真实数据
    user_id = rng.zipf(1.5, n) % args.users + 1
    item_id = rng.zipf(1.3, n) % args.items + 1
    category_id = item_id % args.categories + 1  # 同一个商品永远属于同一个类目
    behavior = rng.choice(BEHAVIORS, size=n, p=BEHAVIOR_P)
    day = rng.integers(0, DAYS, n)
    hour = rng.choice(24, size=n, p=HOUR_WEIGHTS / HOUR_WEIGHTS.sum())
    second = rng.integers(0, 3600, n)
    ts = START_TS + day * 86400 + hour * 3600 + second

    with open(args.out, "w", encoding="utf-8") as f:
        for u, i, c, b, t in zip(user_id, item_id, category_id, behavior, ts):
            f.write(f"{u},{i},{c},{b},{t}\n")
    print(f"已生成 {n:,} 行 → {args.out}")


if __name__ == "__main__":
    main()
