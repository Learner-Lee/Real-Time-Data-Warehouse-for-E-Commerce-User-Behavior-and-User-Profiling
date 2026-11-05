-- R1 的 Doris 表（单个 BE，副本数都是 1）。执行：mysql -h bigdata01 -P 9030 -uroot < doris_tables.sql
CREATE DATABASE IF NOT EXISTS r1_dw;
USE r1_dw;

-- ODS 明细：Routine Load 直接从 Kafka 的 ods_user_behavior 导入，包含迟到数据，用于离线重算和对账
CREATE TABLE IF NOT EXISTS ods_user_behavior (
  ts          BIGINT      COMMENT '事件时间（毫秒）',
  user_id     BIGINT,
  item_id     BIGINT,
  category_id BIGINT,
  behavior    VARCHAR(8),
  late_delay  INT         COMMENT '回放器注入的迟到秒数，正常数据为空'
)
DUPLICATE KEY (ts, user_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- DWS（Flink SQL）：分钟 × 类目的流量指标
CREATE TABLE IF NOT EXISTS dws_traffic_category_1m (
  window_start DATETIME(3),
  category_id  BIGINT,
  pv           BIGINT,
  cart_cnt     BIGINT,
  fav_cnt      BIGINT,
  buy_cnt      BIGINT,
  uv           BIGINT COMMENT '分钟内去重用户数，不能直接跨分钟相加，跨时间段的 UV 用下面的 BITMAP 表'
)
UNIQUE KEY (window_start, category_id)
DISTRIBUTED BY HASH (category_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- DWS（Flink SQL）：分钟 × 类目的用户 BITMAP。任意时间粒度合并都能得到精确 UV，重复导入也不会多算
CREATE TABLE IF NOT EXISTS dws_uv_bitmap_1m (
  window_start DATETIME(3),
  category_id  BIGINT,
  user_id      BITMAP BITMAP_UNION
)
AGGREGATE KEY (window_start, category_id)
DISTRIBUTED BY HASH (category_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- DWS（Flink SQL）：分钟级交易指标，来自 MySQL CDC 订单；退款会让已写入的数字回撤
CREATE TABLE IF NOT EXISTS dws_trade_1m (
  minute_start DATETIME(3),
  order_cnt    BIGINT,
  gmv          DECIMAL(16, 2),
  refund_cnt   BIGINT
)
UNIQUE KEY (minute_start)
DISTRIBUTED BY HASH (minute_start) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- DataStream 作业：分钟 × 行为类型的计数。允许迟到 1 分钟，迟到数据到达时窗口再次触发，新结果覆盖旧结果
CREATE TABLE IF NOT EXISTS dws_behavior_1m_stream (
  window_start DATETIME(3),
  behavior     VARCHAR(8),
  cnt          BIGINT,
  emit_time    DATETIME(3) COMMENT '这次结果的输出时间，可以看出窗口触发了几次'
)
UNIQUE KEY (window_start, behavior)
DISTRIBUTED BY HASH (behavior) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- DataStream 作业的侧输出：超过允许迟到时间的数据，单独保存，用于补算
CREATE TABLE IF NOT EXISTS dwd_late_events (
  ts          BIGINT,
  user_id     BIGINT,
  item_id     BIGINT,
  category_id BIGINT,
  behavior    VARCHAR(8),
  late_delay  INT
)
DUPLICATE KEY (ts, user_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
