-- 冒烟测试用的 Doris 结果表。单个 BE，副本数必须设成 1
CREATE DATABASE IF NOT EXISTS r1_smoke;
USE r1_smoke;

-- 链路 1：Kafka → Doris
DROP TABLE IF EXISTS kafka_events;
CREATE TABLE kafka_events (
  id      BIGINT,
  user_id VARCHAR(32),
  action  VARCHAR(32),
  ts      BIGINT
)
UNIQUE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 链路 2：MySQL CDC → Doris（Unique 模型，才能接住上游的更新和删除）
DROP TABLE IF EXISTS mysql_orders;
CREATE TABLE mysql_orders (
  order_id    BIGINT,
  user_id     VARCHAR(32),
  amount      DECIMAL(10, 2),
  update_time DATETIME(3)
)
UNIQUE KEY (order_id)
DISTRIBUTED BY HASH (order_id) BUCKETS 1
PROPERTIES ("replication_num" = "1");

-- 链路 3：Kafka 关联 HBase 维表 → Doris
DROP TABLE IF EXISTS enriched_events;
CREATE TABLE enriched_events (
  id      BIGINT,
  user_id VARCHAR(32),
  level   VARCHAR(32)
)
UNIQUE KEY (id)
DISTRIBUTED BY HASH (id) BUCKETS 1
PROPERTIES ("replication_num" = "1");
