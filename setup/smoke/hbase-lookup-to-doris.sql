-- 冒烟测试 3：Kafka 事件关联 HBase 维表（异步 Lookup Join）→ Doris。@...@ 由 run-smoke.sh 替换
-- 这一项同时验证：HBase 连接器官方只发布到适配 Flink 1.19 的版本，在 1.20 上能不能用
SET 'pipeline.name' = 'smoke-hbase-lookup-doris';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE events (
  id        BIGINT,
  user_id   STRING,
  action    STRING,
  ts        BIGINT,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'smoke_events',
  'properties.bootstrap.servers' = '@VM_HOST@:9092',
  'properties.group.id' = 'smoke-hbase-lookup',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

-- HBase 维表：rowkey 是 user_id，列族 f 里有 level 一列；开启异步查询和本地缓存
CREATE TABLE dim_user (
  rowkey STRING,
  f      ROW<level STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'hbase-2.2',
  'table-name' = 'smoke_dim',
  'zookeeper.quorum' = '@VM_HOST@:2181',
  'lookup.async' = 'true',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '10000',
  'lookup.partial-cache.expire-after-write' = '10 min'
);

CREATE TABLE doris_enriched (
  id      BIGINT,
  user_id STRING,
  level   STRING
) WITH (
  'connector' = 'doris',
  'fenodes' = '@VM_HOST@:8030',
  'table.identifier' = 'r1_smoke.enriched_events',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'smoke_hbase_@LABEL_SUFFIX@'
);

INSERT INTO doris_enriched
SELECT e.id, e.user_id, d.f.level
FROM events AS e
LEFT JOIN dim_user FOR SYSTEM_TIME AS OF e.proc_time AS d
  ON e.user_id = d.rowkey;
