-- Flink SQL：MySQL CDC 订单 → DWS 交易（分钟级 GMV）→ Doris。@...@ 由 bin/start_jobs.sh 替换
-- CDC 是变更流：订单退款时，上游发来「旧值撤回 + 新值」，聚合结果会跟着回撤；
-- 所以这里用普通的 GROUP BY（支持更新），不用窗口；Doris 用 Unique 模型按主键覆盖
SET 'pipeline.name' = 'r1-dws-trade';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';

CREATE TABLE ods_orders (
  order_id    BIGINT,
  user_id     BIGINT,
  item_id     BIGINT,
  amount      DECIMAL(10, 2),
  status      STRING,
  create_time TIMESTAMP(3),
  update_time TIMESTAMP(3),
  PRIMARY KEY (order_id) NOT ENFORCED
) WITH (
  'connector' = 'mysql-cdc',
  'hostname' = '@VM_HOST@',
  'port' = '3306',
  'username' = '@MYSQL_APP_USER@',
  'password' = '@MYSQL_APP_PASSWORD@',
  'database-name' = 'r1_shop',
  'table-name' = 'orders',
  'server-time-zone' = 'Asia/Shanghai'
);

CREATE TABLE doris_dws_trade_1m (
  minute_start TIMESTAMP(3),
  order_cnt    BIGINT,
  gmv          DECIMAL(16, 2),
  refund_cnt   BIGINT,
  PRIMARY KEY (minute_start) NOT ENFORCED
) WITH (
  'connector' = 'doris',
  'fenodes' = '@VM_HOST@:8030',
  'table.identifier' = 'r1_dw.dws_trade_1m',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'r1_dws_trade_@LABEL_SUFFIX@'
);

INSERT INTO doris_dws_trade_1m
SELECT
  FLOOR(create_time TO MINUTE) AS minute_start,
  COUNT(*) FILTER (WHERE status = 'PAID') AS order_cnt,
  CAST(COALESCE(SUM(amount) FILTER (WHERE status = 'PAID'), 0) AS DECIMAL(16, 2)) AS gmv,
  COUNT(*) FILTER (WHERE status = 'REFUNDED') AS refund_cnt
FROM ods_orders
GROUP BY FLOOR(create_time TO MINUTE);
