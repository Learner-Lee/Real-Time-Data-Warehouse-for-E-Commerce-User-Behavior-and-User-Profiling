-- 冒烟测试 2：MySQL CDC（先读全量，再接着读 binlog 增量）→ Flink SQL → Doris。@...@ 由 run-smoke.sh 替换
SET 'pipeline.name' = 'smoke-mysql-cdc-doris';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '10s';

CREATE TABLE mysql_orders (
  order_id    BIGINT,
  user_id     STRING,
  amount      DECIMAL(10, 2),
  update_time TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '@VM_HOST@',
  'port' = '3306',
  'username' = '@MYSQL_APP_USER@',
  'password' = '@MYSQL_APP_PASSWORD@',
  'database-name' = 'r1_shop',
  'table-name' = 'smoke_orders',
  'server-time-zone' = 'Asia/Shanghai'
);

CREATE TABLE doris_orders (
  order_id    BIGINT,
  user_id     STRING,
  amount      DECIMAL(10, 2),
  update_time TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes' = '@VM_HOST@:8030',
  'table.identifier' = 'r1_smoke.mysql_orders',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'smoke_cdc_@LABEL_SUFFIX@'
);

INSERT INTO doris_orders SELECT order_id, user_id, amount, update_time FROM mysql_orders;
