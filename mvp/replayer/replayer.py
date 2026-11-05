"""R1 回放器：按事件时间把用户行为回放到 Kafka；buy 行为同时写进 MySQL 订单表（模拟业务库）。

- 节奏：--speed 600 表示 1 秒回放 600 秒的事件时间；--speed 0 表示不按事件时间控速，
  只受 --max-rate 限制（压测峰值吞吐时用）
- 乱序：每条事件随机推迟 0 到 --disorder 秒发送，Watermark 能兜住
- 迟到：--late "0.005:30,0.01:180" 表示 0.5% 的事件迟到 30 秒（在 allowedLateness 以内，会让窗口再次触发），
  1% 迟到 180 秒（Watermark 容忍 5 秒 + 允许迟到 60 秒，180 秒一定超出，进侧输出）。
  迟到的事件带 "_late" 字段，方便对账
- 订单：每个 buy 生成一条 PAID 订单（写库时间就是事件时间，日志晚到不影响业务库）；
  --refund-ratio 比例的订单在 10 分钟（事件时间）后改成 REFUNDED，用来验证 CDC 的更新和指标回撤
- --dry-run 目录：不连 Kafka 和 MySQL，把消息和订单操作写到本地文件，没有环境时也能检查回放逻辑

MySQL 密码从环境变量 MYSQL_APP_PASSWORD 读取，不放在命令行里（命令行参数别人用 ps 能看到）。
"""
import argparse
import csv
import heapq
import json
import os
import random
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

TZ = timezone(timedelta(hours=8))
REFUND_DELAY_MS = 10 * 60 * 1000
DEFAULT_PRICE = 9.90


def parse_args():
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--input", default="/data/r1/behavior_sorted.csv", help="按时间排好序的回放文件")
    parser.add_argument("--dim-item", default="/data/r1/dim_item.csv", help="商品维表，用来给订单定价")
    parser.add_argument("--speed", type=float, default=600, help="1 秒回放多少秒的事件时间；0 表示不按事件时间控速")
    parser.add_argument("--max-rate", type=int, default=0, help="每秒最多发多少条，0 表示不限")
    parser.add_argument("--disorder", type=int, default=3, help="随机乱序的最大秒数")
    parser.add_argument("--late", default="0.005:30,0.01:180", help="迟到规则「比例:秒数」，多个用逗号隔开")
    parser.add_argument("--refund-ratio", type=float, default=0.02, help="订单退款比例")
    parser.add_argument("--seed", type=int, default=7, help="随机种子，相同种子回放出的数据相同")
    parser.add_argument("--bootstrap-servers", default="bigdata01:9092")
    parser.add_argument("--topic", default="ods_user_behavior")
    parser.add_argument("--mysql-host", default="bigdata01")
    parser.add_argument("--mysql-user", default="r1")
    parser.add_argument("--dry-run", metavar="目录", help="不连 Kafka 和 MySQL，结果写到这个目录")
    return parser.parse_args()


def parse_late(spec):
    """'0.005:30,0.01:120' → [(0.005, 30), (0.01, 120)]"""
    rules = []
    for part in filter(None, spec.split(",")):
        ratio, delay = part.split(":")
        rules.append((float(ratio), int(delay)))
    return rules


def load_prices(path):
    prices = {}
    if os.path.exists(path):
        with open(path, newline="", encoding="utf-8") as f:
            for item_id, _category_id, price in csv.reader(f):
                prices[int(item_id)] = float(price)
    return prices


def to_local_datetime(ts_ms):
    """事件时间（毫秒）→ 北京时间的 datetime（不带时区，直接写 MySQL 的 DATETIME）。"""
    return datetime.fromtimestamp(ts_ms / 1000, TZ).replace(tzinfo=None)


class LiveSinks:
    """真正的输出：行为日志写 Kafka，订单写 MySQL。"""

    def __init__(self, args):
        from confluent_kafka import Producer
        import pymysql

        self.producer = Producer({
            "bootstrap.servers": args.bootstrap_servers,
            "enable.idempotence": True,          # 幂等生产者：重试不会产生重复消息
            "acks": "all",
            "linger.ms": 20,                     # 攒 20 毫秒再发，换取吞吐
            "compression.type": "lz4",
            "queue.buffering.max.messages": 1_000_000,
            # 用和 Java 客户端（Flink Kafka sink）相同的 murmur2 算分区：同一用户在 ODS 和 DWD 里落在同一个分区号。
            # librdkafka 默认是 CRC32，和 Java 不一致；不一致时 DWD 补数据会把几个 ODS 分区的数据混进同一个分区，事件时间被打乱
            "partitioner": "murmur2_random",
        })
        self.topic = args.topic
        self.db = pymysql.connect(host=args.mysql_host, user=args.mysql_user,
                                  password=os.environ.get("MYSQL_APP_PASSWORD", ""),
                                  database="r1_shop", charset="utf8mb4", autocommit=False)
        self.cursor = self.db.cursor()
        self.order_buffer = []
        self.delivery_errors = 0

    def _on_delivery(self, err, _msg):
        if err is not None:
            self.delivery_errors += 1

    def send_event(self, event):
        value = json.dumps(event, separators=(",", ":"))
        while True:
            try:
                self.producer.produce(self.topic, key=str(event["user_id"]), value=value, on_delivery=self._on_delivery)
                break
            except BufferError:
                self.producer.poll(0.05)  # 本地队列满了：等后台线程把消息发出去
        self.producer.poll(0)

    def insert_order(self, order_id, event, amount):
        created = to_local_datetime(event["ts"])
        self.order_buffer.append((order_id, event["user_id"], event["item_id"], amount, "PAID", created, created))
        if len(self.order_buffer) >= 500:
            self.flush_orders()

    def refund_order(self, order_id, ts_ms):
        self.flush_orders()  # 先把缓冲里的订单写进去，避免 UPDATE 找不到行
        self.cursor.execute("UPDATE orders SET status = 'REFUNDED', update_time = %s WHERE order_id = %s",
                            (to_local_datetime(ts_ms), order_id))
        self.db.commit()

    def flush_orders(self):
        if not self.order_buffer:
            return
        # ON DUPLICATE KEY UPDATE：重复回放同一份数据时不会因主键冲突报错
        self.cursor.executemany(
            "INSERT INTO orders (order_id, user_id, item_id, amount, status, create_time, update_time) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s) "
            "ON DUPLICATE KEY UPDATE status = VALUES(status), amount = VALUES(amount), update_time = VALUES(update_time)",
            self.order_buffer)
        self.db.commit()
        self.order_buffer.clear()

    def flush(self):
        self.flush_orders()
        self.producer.poll(0)

    def close(self):
        self.flush_orders()
        remaining = self.producer.flush(30)
        if remaining:
            print(f"[警告] 还有 {remaining} 条消息没发出去", file=sys.stderr)
        self.db.close()


class DryRunSinks:
    """演练模式：不连任何服务，消息写 events.jsonl，订单操作写 orders.csv。"""

    def __init__(self, directory):
        os.makedirs(directory, exist_ok=True)
        self.events = open(os.path.join(directory, "events.jsonl"), "w", encoding="utf-8")
        self.orders = open(os.path.join(directory, "orders.csv"), "w", encoding="utf-8")
        self.delivery_errors = 0

    def send_event(self, event):
        self.events.write(json.dumps(event, separators=(",", ":")) + "\n")

    def insert_order(self, order_id, event, amount):
        self.orders.write(f"INSERT,{order_id},{event['user_id']},{event['item_id']},{amount},PAID,{to_local_datetime(event['ts'])}\n")

    def refund_order(self, order_id, ts_ms):
        self.orders.write(f"REFUND,{order_id},,,,REFUNDED,{to_local_datetime(ts_ms)}\n")

    def flush(self):
        pass

    def close(self):
        self.events.close()
        self.orders.close()


class Stats:
    def __init__(self):
        self.start = time.time()
        self.sent = 0
        self.per_second = Counter()   # 每个墙钟秒发出的条数，用来算峰值
        self.late = Counter()
        self.orders = 0
        self.refunds = 0
        self.last_report = self.start
        self.last_sent = 0

    def on_sent(self):
        self.sent += 1
        self.per_second[int(time.time())] += 1

    def peak(self):
        return max(self.per_second.values(), default=0)

    def maybe_report(self, pending, sinks):
        now = time.time()
        if now - self.last_report < 5:
            return False
        rate = (self.sent - self.last_sent) / (now - self.last_report)
        late = "、".join(f"迟到 {d} 秒 {n:,} 条" for d, n in sorted(self.late.items())) or "无"
        print(f"[{time.strftime('%H:%M:%S')}] 已发 {self.sent:,} 条｜当前 {rate:,.0f} 条/秒｜峰值 {self.peak():,} 条/秒｜"
              f"待发 {pending:,}｜{late}｜订单 {self.orders:,}｜退款 {self.refunds:,}｜发送失败 {sinks.delivery_errors}",
              flush=True)
        self.last_report, self.last_sent = now, self.sent
        return True

    def summary(self):
        elapsed = max(time.time() - self.start, 1e-9)
        return (f"共发送 {self.sent:,} 条，用时 {elapsed:,.1f} 秒，平均 {self.sent / elapsed:,.0f} 条/秒，"
                f"峰值 {self.peak():,} 条/秒；迟到 {sum(self.late.values()):,} 条（{dict(self.late)}）；"
                f"订单 {self.orders:,}，退款 {self.refunds:,}")


def main():
    args = parse_args()
    late_rules = parse_late(args.late)
    rng = random.Random(args.seed)
    prices = load_prices(args.dim_item)
    sinks = DryRunSinks(args.dry_run) if args.dry_run else LiveSinks(args)
    stats = Stats()
    pending = []   # 待发送的事件：(发送时刻, 序号, 事件)，发送时刻按事件时间算
    refunds = []   # 待退款的订单：(退款时刻, 订单号)
    seq = 0
    order_id = 0
    first_ts = None

    def emit_due(now_ms):
        """回放时钟走到 now_ms：发出所有到点的事件，执行到点的退款。"""
        while pending and pending[0][0] <= now_ms:
            _, _, event = heapq.heappop(pending)
            sinks.send_event(event)
            stats.on_sent()
            throttle()
        while refunds and refunds[0][0] <= now_ms:
            refund_at, refund_id = heapq.heappop(refunds)
            sinks.refund_order(refund_id, refund_at)
            stats.refunds += 1

    def throttle():
        """--max-rate 限速：发得比目标快就睡一会儿。"""
        if args.max_rate > 0 and stats.sent % 100 == 0:
            ahead = stats.sent / args.max_rate - (time.time() - stats.start)
            if ahead > 0.005:
                time.sleep(ahead)

    def pace(ts_ms):
        """--speed 控速：让墙钟时间和事件时间按比例前进。"""
        if args.speed > 0:
            ahead = (ts_ms - first_ts) / 1000 / args.speed - (time.time() - stats.start)
            if ahead > 0.005:
                time.sleep(ahead)

    print(f"开始回放 {args.input}（速度 {args.speed}×，限速 {args.max_rate or '不限'} 条/秒，"
          f"{'演练模式 → ' + args.dry_run if args.dry_run else 'Kafka ' + args.bootstrap_servers + ' / ' + args.topic}）",
          flush=True)
    try:
        with open(args.input, newline="", encoding="utf-8") as f:
            for user_id, item_id, category_id, behavior, ts_s in csv.reader(f):
                ts_ms = int(ts_s) * 1000 + rng.randrange(1000)  # 原始数据精确到秒，补上随机毫秒
                if first_ts is None:
                    first_ts = ts_ms
                pace(ts_ms)

                event = {"user_id": int(user_id), "item_id": int(item_id), "category_id": int(category_id),
                         "behavior": behavior, "ts": ts_ms}
                send_at = ts_ms + rng.randrange(args.disorder * 1000 + 1)
                for ratio, delay in late_rules:
                    if rng.random() < ratio:
                        send_at = ts_ms + delay * 1000
                        event["_late"] = delay
                        stats.late[delay] += 1
                        break
                heapq.heappush(pending, (send_at, seq, event))
                seq += 1

                if behavior == "buy":
                    order_id += 1
                    sinks.insert_order(order_id, event, prices.get(int(item_id), DEFAULT_PRICE))
                    stats.orders += 1
                    if rng.random() < args.refund_ratio:
                        heapq.heappush(refunds, (ts_ms + REFUND_DELAY_MS, order_id))

                emit_due(ts_ms)
                if stats.maybe_report(len(pending), sinks):
                    sinks.flush()

        emit_due(float("inf"))  # 文件读完：剩下的事件和退款全部按顺序发出
    except KeyboardInterrupt:
        print("\n收到 Ctrl+C，停止回放", flush=True)
    finally:
        sinks.close()
        print(stats.summary(), flush=True)


if __name__ == "__main__":
    main()
