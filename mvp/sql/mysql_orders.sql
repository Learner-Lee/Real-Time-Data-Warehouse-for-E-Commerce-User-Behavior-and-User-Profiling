-- R1 业务库的订单表（在 MySQL 的 r1_shop 库执行）。回放器写入，Flink CDC 同步
CREATE TABLE IF NOT EXISTS orders (
  order_id    BIGINT         NOT NULL PRIMARY KEY,
  user_id     BIGINT         NOT NULL,
  item_id     BIGINT         NOT NULL,
  amount      DECIMAL(10, 2) NOT NULL,
  status      VARCHAR(16)    NOT NULL COMMENT 'PAID 已支付 / REFUNDED 已退款',
  create_time DATETIME(3)    NOT NULL,
  update_time DATETIME(3)    NOT NULL,
  KEY idx_create_time (create_time)
) ENGINE = InnoDB DEFAULT CHARSET = utf8mb4;
