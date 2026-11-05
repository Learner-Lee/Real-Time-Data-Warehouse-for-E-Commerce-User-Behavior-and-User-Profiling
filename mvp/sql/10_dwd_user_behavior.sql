-- Flink SQL：ODS → DWD。清洗 + 异步关联 HBase 用户维表和商品维表，结果写回 Kafka。@...@ 由 bin/start_jobs.sh 替换
SET 'pipeline.name' = 'r1-dwd-user-behavior';
-- 单节点只有 4 个 slot，R1 的 4 个作业各占 1 个；想提高并行度先在 r1.env 里调大 FLINK_TM_SLOTS
SET 'parallelism.default' = '1';
-- Kafka 精确一次写入要靠 checkpoint 提交事务：下游最多晚一个 checkpoint 间隔看到数据
SET 'execution.checkpointing.interval' = '30s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';
-- 异步 Lookup 的结果按输入顺序输出（默认值，写出来是为了说明它是分区有序的前提）
SET 'table.exec.async-lookup.output-mode' = 'ORDERED';

CREATE TABLE ods_user_behavior (
  user_id     BIGINT,
  item_id     BIGINT,
  category_id BIGINT,
  behavior    STRING,
  ts          BIGINT,
  proc_time AS PROCTIME()
) WITH (
  'connector' = 'kafka',
  'topic' = 'ods_user_behavior',
  'properties.bootstrap.servers' = '@VM_HOST@:9092',
  'properties.group.id' = 'r1-dwd',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json',
  'json.ignore-parse-errors' = 'true'
);

-- HBase 维表：异步查询 + 本地缓存（部分缓存：只缓存查过的 key，1 小时过期）
CREATE TABLE dim_user (
  rowkey STRING,
  f      ROW<level STRING, city STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'hbase-2.2',
  'table-name' = 'dim_user',
  'zookeeper.quorum' = '@VM_HOST@:2181',
  'lookup.async' = 'true',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '200000',
  'lookup.partial-cache.expire-after-write' = '1 h'
);

CREATE TABLE dim_item (
  rowkey STRING,
  f      ROW<category_id BIGINT, price DECIMAL(10, 2)>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'hbase-2.2',
  'table-name' = 'dim_item',
  'zookeeper.quorum' = '@VM_HOST@:2181',
  'lookup.async' = 'true',
  'lookup.cache' = 'PARTIAL',
  'lookup.partial-cache.max-rows' = '500000',
  'lookup.partial-cache.expire-after-write' = '1 h'
);

-- DWD 明细写回 Kafka，用事务实现精确一次（broker 的事务超时上限已在环境脚本里调到 1 小时）
-- 分区保序：消息 key 和回放器写 ODS 时一样（user_id 的字符串），分区算法都是 murmur2，
-- 所以 ODS 分区 p 的数据只会进 DWD 分区 p，而且顺序不变。故障恢复后补数据时，
-- 即使 3 个 ODS 分区读得快慢不一，每个 DWD 分区内部仍然按事件时间有序，下游按分区算的 Watermark 不会误判迟到
CREATE TABLE dwd_user_behavior (
  user_key    STRING,
  user_id     BIGINT,
  item_id     BIGINT,
  category_id BIGINT,
  behavior    STRING,
  ts          BIGINT,
  user_level  STRING,
  city        STRING,
  price       DECIMAL(10, 2)
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_user_behavior',
  'properties.bootstrap.servers' = '@VM_HOST@:9092',
  'key.format' = 'raw',
  'key.fields' = 'user_key',
  'value.format' = 'json',
  'value.fields-include' = 'EXCEPT_KEY',
  'sink.partitioner' = 'default',
  'sink.delivery-guarantee' = 'exactly-once',
  'sink.transactional-id-prefix' = 'r1-dwd-@LABEL_SUFFIX@',
  'properties.transaction.timeout.ms' = '900000'
);

-- 用和导入维表时相同的公式算出加盐后的 rowkey，再去 HBase 点查
INSERT INTO dwd_user_behavior
SELECT CAST(o.user_id AS STRING), o.user_id, o.item_id, o.category_id, o.behavior, o.ts, u.f.level, u.f.city, i.f.price
FROM ods_user_behavior AS o
LEFT JOIN dim_user FOR SYSTEM_TIME AS OF o.proc_time AS u
  ON u.rowkey = CONCAT(CAST(MOD(o.user_id, 10) AS STRING), '_', CAST(o.user_id AS STRING))
LEFT JOIN dim_item FOR SYSTEM_TIME AS OF o.proc_time AS i
  ON i.rowkey = CONCAT(CAST(MOD(o.item_id, 10) AS STRING), '_', CAST(o.item_id AS STRING))
WHERE o.user_id IS NOT NULL
  AND o.ts IS NOT NULL
  AND o.behavior IN ('pv', 'cart', 'fav', 'buy');
