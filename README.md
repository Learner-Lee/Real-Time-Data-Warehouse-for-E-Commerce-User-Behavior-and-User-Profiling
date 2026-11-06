# 电商用户行为实时数仓

在一台 Ubuntu 虚拟机（16GB 内存，ARM64）上，从安装组件开始，完整搭建一条实时数仓链路：
- **数据**：回放器按事件时间回放 9 天、50 万条用户行为，并故意注入乱序和迟到；
- **计算**：Flink SQL 和 DataStream 两套作业；
- **存储**：Kafka、HBase 维表和 Doris；
- **验证**：用三种对账证明结果正确，强杀 TaskManager 后数据不丢、不重复。

> 版本：Flink 1.20.5、Kafka 4.3.2（KRaft）、HBase 2.6.7、Doris 4.0.8、MySQL 8.0、JDK 17

## 结论

| 验证项 | 结果 |
|---|---|
| SQL 窗口 vs 离线重算（对账 A） | 447,482 条 pv 只差 **62 条（0.014%）**，差的都是按设计丢弃的迟到数据 |
| DataStream 三方对账（对账 B） | 离线 = 实时 + 侧输出，`unexplained = 0` |
| 精确一次 | 回放中途 `kill -9` TaskManager，4 个作业从 checkpoint 自动恢复；GMV 10,333 单、921,384.16 元，**和 MySQL 一分不差** |
| CDC 退款回撤 | 订单退款后，对应分钟的 GMV 被撤回 |
| UV 精确去重 | 分钟 UV 直接相加会多算约 10.7 倍；BITMAP 合并后精确 |
| 端到端延迟 | Kafka → Doris 汇总表约 18 秒，主要来自 30 秒的 checkpoint 间隔 |
| 分区有序 | 故障后追数据时，DWD 各分区的乱序比例和 ODS 完全一致 |

详细数字和分析见 [docs/06.9-R1-第一轮实验结果.md](docs/06.9-R1-第一轮实验结果.md)。

## 架构

![架构图：回放器写入 Kafka 和 MySQL，Flink SQL、Flink CDC、Java DataStream 三条链路处理后写入 Doris，支撑大屏和对账](assets/architecture.svg)

<details>
<summary>文字版（带 topic 名和表名）</summary>

```
回放器（Python）──行为日志──→ Kafka ods_user_behavior ──┬──→ Flink SQL：DWD（清洗 + 异步关联 HBase 维表）──→ Kafka dwd_user_behavior
      │                                                 │                                                        │
      │ buy 行为写订单、模拟退款                          │                                         Flink SQL：DWS 流量（1 分钟窗口）
      ↓                                                 │                                                        ↓
MySQL r1_shop.orders ──→ Flink CDC ──→ Flink SQL：DWS 交易（GMV，退款会回撤）──→ Doris dws_trade_1m       Doris dws_traffic_category_1m
                                                        │                                                Doris dws_uv_bitmap_1m（BITMAP）
                                                        │
                                                        ├──→ Java DataStream：Watermark + allowedLateness + 侧输出
                                                        │        ├─ 窗口结果 → Kafka → Routine Load → Doris dws_behavior_1m_stream
                                                        │        └─ 侧输出   → Kafka → Routine Load → Doris dwd_late_events
                                                        │
                                                        └──→ Routine Load → Doris ods_user_behavior（离线重算、对账用）

HBase dim_user / dim_item：维表（加盐 rowkey + 预分区），由 Flink 作业从 CSV 导入
```

</details>

## 目录

| 路径 | 内容 |
|---|---|
| [`setup/`](setup/README.md) | 环境安装脚本：00–06 依次装基础环境、MySQL、Kafka、HBase、Doris、Flink、Python，带启停脚本和冒烟测试 |
| [`mvp/`](mvp/README.md) | 数仓本身：数据准备、回放器、Flink SQL、Java DataStream 作业、Doris 建表和对账 SQL、运行脚本 |
| [`docs/`](docs/) | 环境方案、每一步的记录、排障过程和实验结果 |

## 怎么跑

1. **准备虚拟机**：按 [docs/00-虚拟机准备.md](docs/00-虚拟机准备.md) 建好 Ubuntu Server 24.04 虚拟机并固定 IP。
2. **装环境**：按 [setup/README.md](setup/README.md) 依次运行 7 个安装脚本，冒烟测试三项都通过即可。
3. **跑数仓**：按 [mvp/README.md](mvp/README.md) 第三节运行 `prepare → setup → start_jobs → replay → check`，再用 `sql/ads_dashboard.sql` 对账。

## 排障记录（每个都有日志证据和修复验证）

| 问题 | 根因 | 记录 |
|---|---|---|
| Lookup Join 一直连不上 ZooKeeper | 连接器内置的 ZooKeeper 3.4 客户端在 JDK 14+ 上解析不了主机名（ZOOKEEPER-3779），给 jar 打了一行补丁 | [06.4](docs/06.4-R1环境排障.md) |
| 故障恢复后实时 UV/PV 掉了 76% | DWD 追数据时打乱了分区顺序：librdkafka（CRC32）和 Java 客户端（murmur2）分区算法不同；另外空闲超时比上游提交间隔短 | [06.6](docs/06.6-R1-分区保序与重跑.md) |
| Doris 实时表 6 分钟不更新，或完全不进数 | 三个问题叠在一起：Doris 4 的自适应批次每批至少 360 秒；事件时间（2017 年）被写成 Kafka 消息时间戳，被保留策略当作过期删掉；Doris 复用了坏掉的 Kafka 消费者 | [06.7](docs/06.7-Doris导入卡住根因.md) |
| HBase 在虚拟机卡顿后退出 | 虚拟机整体停顿 23 秒，超过 ZooKeeper 会话超时（默认 10 秒） | [06.5](docs/06.5-R1-MVP首次运行.md) |

## 还没做的

- 实验 1 峰值吞吐（`replay.sh --speed 0 --max-rate 20000`）、实验 5 维表缓存对吞吐的影响。
- Flume 采集、Superset / Grafana 大屏、用户画像模型（RFM + K-Means）。
