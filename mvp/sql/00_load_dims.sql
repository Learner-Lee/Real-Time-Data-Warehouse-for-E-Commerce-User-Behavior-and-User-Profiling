-- Flink 作业：维表 CSV → HBase。@...@ 由 bin/setup.sh 替换
-- rowkey 加盐：(id % 10) + '_' + id，和 hbase_tables.rb 的预分区对应
-- 用默认的流模式读有界文件：文件读完作业就结束（HBase 的写入走流式 sink，流模式下最稳）
SET 'pipeline.name' = 'r1-load-dims';
SET 'parallelism.default' = '1';
-- 让 sql-client 等批作业跑完再退出，setup.sh 才能接着检查结果
SET 'table.dml-sync' = 'true';

CREATE TABLE dim_user_csv (
  user_id BIGINT,
  level   STRING,
  city    STRING
) WITH (
  'connector' = 'filesystem',
  'path' = 'file://@DATA_DIR@/r1/dim_user.csv',
  'format' = 'csv'
);

CREATE TABLE dim_item_csv (
  item_id     BIGINT,
  category_id BIGINT,
  price       DECIMAL(10, 2)
) WITH (
  'connector' = 'filesystem',
  'path' = 'file://@DATA_DIR@/r1/dim_item.csv',
  'format' = 'csv'
);

CREATE TABLE hbase_dim_user (
  rowkey STRING,
  f      ROW<level STRING, city STRING>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'hbase-2.2',
  'table-name' = 'dim_user',
  'zookeeper.quorum' = '@VM_HOST@:2181'
);

CREATE TABLE hbase_dim_item (
  rowkey STRING,
  f      ROW<category_id BIGINT, price DECIMAL(10, 2)>,
  PRIMARY KEY (rowkey) NOT ENFORCED
) WITH (
  'connector' = 'hbase-2.2',
  'table-name' = 'dim_item',
  'zookeeper.quorum' = '@VM_HOST@:2181'
);

EXECUTE STATEMENT SET
BEGIN
INSERT INTO hbase_dim_user
SELECT CONCAT(CAST(MOD(user_id, 10) AS STRING), '_', CAST(user_id AS STRING)), ROW(level, city)
FROM dim_user_csv;
INSERT INTO hbase_dim_item
SELECT CONCAT(CAST(MOD(item_id, 10) AS STRING), '_', CAST(item_id AS STRING)), ROW(category_id, price)
FROM dim_item_csv;
END;
