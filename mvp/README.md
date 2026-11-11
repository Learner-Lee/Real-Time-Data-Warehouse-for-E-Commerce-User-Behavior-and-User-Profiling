# R1 复刻：电商用户行为实时数仓（MVP）

> 对应简历：项目「电商用户行为实时数仓与用户画像」｜复刻手册 R1｜环境：[setup](../setup/README.md)
> 版本：Flink 1.20.5、Kafka 4.3.2、HBase 2.6.7、Doris 4.0.8、MySQL 8.0

## 结论

MVP 跑通「回放 → Kafka → Flink（SQL + DataStream）→ Doris」整条链路，覆盖简历里的这些说法：
- Flink CDC 同步业务库；
- Flink SQL 构建实时 DWD / DWS；
- Watermark + 侧输出处理乱序和迟到；
- HBase 维表异步 Lookup Join；
- Doris 大屏（UV / GMV / 转化漏斗）；
- Checkpoint + Kafka 两阶段提交实现 Exactly-Once。

**留给完整版**：Flume 采集。大屏（Grafana，见环境脚本 `07-grafana.sh`）和用户画像链路（`bin/profile.sh`，目前用模拟数据跑通）已补上。

**已在 Mac 上验证的**（没有虚拟机也能跑的部分）：
- Java 作业 3 个单元测试通过：三道防线的结果和设计一致。
- 4 个 Flink SQL 文件全部通过 Flink 1.20.5 规划器的校验。
- 数据准备脚本和回放器用 5 万行模拟数据跑通：迟到注入比例、乱序幅度、订单数都和设计一致。

**还没验证的**：在虚拟机上真正跑起来，以你那边的结果为准。

---

## 一、架构

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

## 二、目录

| 路径 | 内容 |
|---|---|
| `data/gen_synthetic.py` | 生成和 UserBehavior 同格式的模拟数据（还没下载真实数据时用） |
| `data/prepare_data.py` | 抽样、按时间排序、生成用户 / 商品维表（DuckDB 处理） |
| `replayer/replayer.py` | 按事件时间回放，注入乱序和迟到；buy 行为写 MySQL 订单并模拟退款；`--dry-run` 不连任何服务 |
| `sql/00_load_dims.sql` | Flink 作业：维表 CSV → HBase |
| `sql/10_dwd_user_behavior.sql` | Flink SQL：ODS → DWD，异步关联 HBase 维表，事务写回 Kafka |
| `sql/20_dws_traffic.sql` | Flink SQL：DWD → 分钟级流量指标 + UV BITMAP → Doris |
| `sql/30_dws_trade.sql` | Flink SQL：MySQL CDC 订单 → 分钟级 GMV → Doris |
| `sql/doris_tables.sql`、`doris_routine_load.sql` | Doris 建表、3 个 Routine Load |
| `sql/hbase_tables.rb`、`mysql_orders.sql` | HBase 维表、MySQL 订单表 |
| `sql/ads_dashboard.sql` | 大屏查询 + 三种对账 |
| `late-data-job/` | Java DataStream 作业（Maven 工程），测试里顺带校验所有 Flink SQL |
| `sql/profile_tables.sql` | 画像：用户 × 天的 DWS 表、标签表，从 ODS 生成 DWS 的批处理 SQL |
| `user_profile/` | 画像：55 个特征的定义和计算（`features.py`）、RFM + K-Means + XGBoost + 标签回写（`run_profile.py`） |
| `bin/` | `prepare`、`setup`、`start_jobs`、`replay`、`check`、`bench_query`、`profile`、`stop_jobs`、`reset` |

## 三、运行步骤

**前提**：R1 环境已按 setup 装好，冒烟测试三项都通过。

> **如果你在 2026-11-02 之前就跑过环境脚本**：先把最新的 `setup` 再复制一次到虚拟机，然后重跑两个脚本：
> - `05-flink.sh`：补上 HBase 连接器需要的 Hadoop 客户端 jar，否则冒烟测试第 3 项和 DWD 作业会报 `NoClassDefFoundError: org/apache/hadoop/io/Writable`；
> - `06-python.sh`：补装 duckdb。
>
> 两个脚本都可以重复运行。跑完后再做一遍冒烟测试。

1. **在 Mac 上打包 Java 作业**（同时跑 8 个测试）：
   ```bash
   mvn -f mvp/late-data-job/pom.xml package   # 在仓库根目录执行，需要 JDK 17
   ```
2. **复制到虚拟机**：
   ```bash
   rsync -a mvp/ bigdata01:~/r1-realtime-dw/   # 在仓库根目录执行
   ```
3. **在虚拟机里按顺序运行**（都在 `~/r1-realtime-dw` 目录下）：

   | 顺序 | 命令 | 说明 |
   |---|---|---|
   | 1 | `bash bin/prepare.sh` | 不带参数：生成 50 万行模拟数据。有真实数据时用 `bash bin/prepare.sh /data/UserBehavior.csv`（默认抽 1%，约 100 万行） |
   | 2 | `bash bin/setup.sh` | 建 Kafka topic、HBase / MySQL / Doris 表，把维表导进 HBase |
   | 3 | `bash bin/start_jobs.sh` | 提交 4 个 Flink 作业，启动 3 个 Routine Load |
   | 4 | `bash bin/replay.sh` | 开始回放（默认 1 秒回放 10 分钟事件时间，9 天数据约 22 分钟放完） |
   | 5 | `bash bin/check.sh` | 看各层数据量、最新数据时间和 Routine Load 状态 |
   | 6 | `sql/ads_dashboard.sql` | 在 Doris 里跑大屏查询；回放结束后跑对账 |
   | 7 | `bash bin/profile.sh` | 回放结束后跑用户画像：DWS 用户表 → 55 个特征 → RFM + K-Means → XGBoost 未来 3 天购买预测 → 标签写回 `ads_user_profile_tag`，报告在 `/data/r1-work/profile/report.md` |

4. **重做实验**：`bash bin/reset.sh`（清空 Doris、Kafka、MySQL 里的 R1 数据，维表和回放文件保留），然后从第 2 步开始。

## 四、实验清单（复刻的重点：每项都记下真实数字）

| # | 实验 | 怎么做 | 记录什么 | 对应的面试追问 |
|---|---|---|---|---|
| 1 | 峰值吞吐 | `bash bin/replay.sh --speed 0 --max-rate 20000` | 回放器打印的峰值；Flink 网页上 DWD 作业的处理速度；有没有反压 | 「Kafka 峰值 2 万条/秒」怎么来的、瓶颈在哪 |
| 2 | 乱序与迟到 | 默认回放（注入 0.5% 迟到 30 秒、1% 迟到 180 秒） | `ads_dashboard.sql` 第 6–9 条的结果 | Watermark 怎么设、迟到数据怎么处理 |
| 3 | 端到端延迟 | 回放过程中多次运行 `check.sh` | ODS 和 DWS 最新时间的差 | 两阶段提交的代价（延迟约等于 checkpoint 间隔） |
| 4 | 精确一次 | 回放中途在虚拟机执行 `kill -9 $(pgrep -f TaskManagerRunner)`，再 `$FLINK_HOME/bin/taskmanager.sh start`，等作业自动恢复 | 回放结束后对账：第 8 条 unexplained 为 0，第 10 条和 MySQL 一致 | Exactly-Once 是怎么保证的 |
| 5 | 维表缓存 | Flink 网页看 DWD 作业的指标；把 `lookup.partial-cache.max-rows` 改小再跑一次 | 处理速度的变化 | 维表 Join 为什么要异步、缓存怎么设 |
| 6 | UV 精确去重 | 同一天的分钟 UV 直接相加，和 BITMAP 合并的结果比较 | 两个数差多少 | 实时 UV 怎么算、为什么不能直接相加 |
| 7 | CDC 回撤 | 回放中盯住某一分钟的 `dws_trade_1m`，10 分钟（事件时间）后它的退款发生 | 退款前后的 GMV | CDC 的更新流、聚合结果怎么回撤 |
| 8 | 查询延迟 | `bash bin/bench_query.sh 100` | P95 | 「Doris 查询 P95 < 1s」怎么测的 |

## 五、和简历的对应

| 简历里的说法 | 在这里的实现 |
|---|---|
| Flume + Kafka 采集埋点日志 | MVP 由回放器直接写 Kafka（幂等生产者、lz4 压缩、批量发送）；Flume 在完整版补上 |
| Flink CDC 同步业务库 | `30_dws_trade.sql` 的 mysql-cdc 源表：先读全量、再无锁切到 binlog 增量 |
| Flink SQL 构建实时 DWD / DWS | `10_dwd_user_behavior.sql`、`20_dws_traffic.sql` |
| Watermark + 侧输出处理乱序与迟到 | SQL 里用 Watermark 容忍 5 秒乱序；侧输出在 Java 作业 `LateDataJob` 里（纯 SQL 做不到侧输出） |
| HBase 维表通过异步 IO 做 Lookup Join | `10_dwd_user_behavior.sql`：`lookup.async` + 部分缓存；rowkey 加盐 + 预分区 |
| Doris 大屏：UV / GMV / 转化漏斗，P95 < 1s | `ads_dashboard.sql` 第 1–4 条；`bench_query.sh` 测 P95 |
| Checkpoint + Kafka 两阶段提交实现 Exactly-Once | DWD 和 Java 作业的 Kafka sink 都开事务；下游用 read_committed 读；Doris sink 也是两阶段提交 |
| 基于 DWS 沉淀 50+ 用户特征，RFM + K-Means 分群，XGBoost 复购预测，结果回写画像标签 | `bin/profile.sh`：55 个特征（`dws_user_feature`）；K 由轮廓系数选（模拟数据上是 5）；按时间切分的测试集 AUC 0.91，但和「只用行为次数」的基线一样，因为模拟数据的购买只和活跃度有关；标签表 `ads_user_profile_tag`。**数字要用真实数据重跑** |
| 状态设 TTL 防膨胀 | MVP 里的窗口状态会随窗口清理自动释放，不需要 TTL；完整版做无限流聚合时再加 |

## 六、设计取舍（面试追问时可以讲）

1. **为什么迟到数据要单独写一个 Java 作业**：
   - Flink SQL 的窗口在 Watermark 越过窗口结束时间就关闭，之后到的数据直接丢，而且 SQL 没有侧输出。
   - DataStream 能用 allowedLateness 让窗口多等一会儿，再用侧输出兜住更晚的数据。
   - 对账 A 和对账 B 的差异，正好说明了两者的区别。
2. **为什么 UV 用 BITMAP**：
   - 分钟 UV 不能直接相加，同一个用户会被重复计算。
   - BITMAP 存的是用户集合，任意时间段合并后都是精确去重；而且重复导入同一批数据也不会多算，合并是幂等的。
3. **为什么 HBase rowkey 加盐**：
   - 连续的 id 会集中写到同一个 region，形成写热点。
   - 盐值用 id 本身算出来（`id % 10`），查询时 Flink 用同样的公式就能点查。
4. **为什么交易指标不用窗口**：
   - CDC 是变更流，退款会撤回已经算过的结果。
   - 普通 GROUP BY 支持更新；Doris 用 Unique 模型，按主键覆盖。
5. **端到端精确一次的链路**：
   - 回放器用幂等生产者写 Kafka；
   - Flink 靠 checkpoint 保证状态一致；
   - 写 Kafka 用事务，在 checkpoint 完成时提交；
   - 下游只读已提交的数据（read_committed）；
   - 写 Doris 也是两阶段提交。
   - 代价是数据最多晚一个 checkpoint 间隔（30 秒）才对下游可见。
6. **为什么有的数据用 Flink 写 Doris，有的用 Routine Load**：
   - Flink 写入适合要在 Flink 里做计算的结果。
   - Routine Load 让 Doris 自己消费 Kafka，适合原样落库的明细。
   - 两种导入方式都练到了。

## 七、常见问题

| 现象 | 原因 / 处理 |
|---|---|
| 作业一直是 CREATED，不运行 | slot 不够：4 个作业各占 1 个，先确认冒烟测试的作业已经取消（Flink 网页上看） |
| DWD 作业报 `NoClassDefFoundError: org/apache/hadoop/io/Writable` | Flink 缺 Hadoop 客户端 jar：用最新的 vm-scripts 重跑 `05-flink.sh` |
| Doris 里 DWS 表没数据 | 先看 DWD topic 有没有数据（`check.sh`）；再看 Watermark 有没有推进：回放要持续进行，窗口才会触发 |
| 回放结束后最后一两分钟没有结果 | 正常现象：没有新数据推动 Watermark，最后的窗口不会触发；对账查询已经排除了最后 3 分钟 |
| Routine Load 变成 PAUSED | `check.sh` 里看 `ReasonOfStateChanged`，多半是 JSON 字段对不上；修好后执行 `RESUME ROUTINE LOAD FOR r1_dw.作业名` |
| 想重新回放 | `bash bin/reset.sh`，然后从 `setup.sh` 开始 |
