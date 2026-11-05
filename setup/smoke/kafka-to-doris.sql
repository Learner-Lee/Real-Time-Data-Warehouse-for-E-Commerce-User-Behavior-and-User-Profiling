-- 冒烟测试 1：Kafka → Flink SQL → Doris。@...@ 由 run-smoke.sh 替换
SET 'pipeline.name' = 'smoke-kafka-doris';
SET 'parallelism.default' = '1';
-- Doris 连接器在 checkpoint 时提交（两阶段提交），不开 checkpoint 数据就不会写进去
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE kafka_events (
  id      BIGINT,
  user_id STRING,
  action  STRING,
  ts      BIGINT
) WITH (
  'connector' = 'kafka',
  'topic' = 'smoke_events',
  'properties.bootstrap.servers' = '@VM_HOST@:9092',
  'properties.group.id' = 'smoke-kafka-doris',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE doris_events (
  id      BIGINT,
  user_id STRING,
  action  STRING,
  ts      BIGINT
) WITH (
  'connector' = 'doris',
  'fenodes' = '@VM_HOST@:8030',
  'table.identifier' = 'r1_smoke.kafka_events',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'smoke_kafka_@LABEL_SUFFIX@'
);

INSERT INTO doris_events SELECT id, user_id, action, ts FROM kafka_events;
