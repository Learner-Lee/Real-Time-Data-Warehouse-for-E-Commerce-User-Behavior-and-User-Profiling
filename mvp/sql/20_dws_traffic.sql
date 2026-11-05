-- Flink SQL：DWD → DWS 流量。1 分钟滚动窗口，结果写 Doris。@...@ 由 bin/start_jobs.sh 替换
-- 注意：SQL 的窗口在 Watermark 越过窗口结束时间后就关闭，之后到的迟到数据会被直接丢弃。
--       迟到数据的处理交给 DataStream 作业（侧输出），两边的差异用 ads_dashboard.sql 里的对账查询看
SET 'pipeline.name' = 'r1-dws-traffic';
SET 'parallelism.default' = '1';
SET 'execution.checkpointing.interval' = '30s';
SET 'table.local-time-zone' = 'Asia/Shanghai';
-- 某个分区一段时间没数据时，不让它拖住全局 Watermark。
-- 必须大于上游 DWD 的事务提交间隔（checkpoint 30 秒）：read_committed 下数据每 30 秒才来一批，
-- 超时设成 10 秒时，批与批之间各分区都会被判成空闲，新一批里先被读到的分区独自把 Watermark 推进几小时事件时间，
-- 其他分区的数据随后到达就被当成迟到丢掉（2026-11-03 实测：多丢了约 2.4 万条 pv）
SET 'table.exec.source.idle-timeout' = '90s';

CREATE TABLE dwd_user_behavior (
  user_id     BIGINT,
  item_id     BIGINT,
  category_id BIGINT,
  behavior    STRING,
  ts          BIGINT,
  user_level  STRING,
  city        STRING,
  price       DECIMAL(10, 2),
  event_time AS TO_TIMESTAMP_LTZ(ts, 3),
  -- 容忍 5 秒乱序
  WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
  'connector' = 'kafka',
  'topic' = 'dwd_user_behavior',
  'properties.bootstrap.servers' = '@VM_HOST@:9092',
  'properties.group.id' = 'r1-dws-traffic',
  -- 上游用事务写入，只读已提交的数据，才能做到端到端精确一次
  'properties.isolation.level' = 'read_committed',
  'scan.startup.mode' = 'earliest-offset',
  'format' = 'json'
);

CREATE TABLE doris_dws_traffic_category_1m (
  window_start TIMESTAMP(3),
  category_id  BIGINT,
  pv           BIGINT,
  cart_cnt     BIGINT,
  fav_cnt      BIGINT,
  buy_cnt      BIGINT,
  uv           BIGINT
) WITH (
  'connector' = 'doris',
  'fenodes' = '@VM_HOST@:8030',
  'table.identifier' = 'r1_dw.dws_traffic_category_1m',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'r1_dws_traffic_@LABEL_SUFFIX@'
);

-- 写 BITMAP 列：Doris 导入时用 to_bitmap(user_id) 把用户 ID 转成 BITMAP，再按聚合模型合并
CREATE TABLE doris_dws_uv_bitmap_1m (
  window_start TIMESTAMP(3),
  category_id  BIGINT,
  user_id      BIGINT
) WITH (
  'connector' = 'doris',
  'fenodes' = '@VM_HOST@:8030',
  'table.identifier' = 'r1_dw.dws_uv_bitmap_1m',
  'username' = 'root',
  'password' = '',
  'sink.label-prefix' = 'r1_dws_uv_@LABEL_SUFFIX@',
  'sink.properties.columns' = 'window_start,category_id,user_id,user_id=to_bitmap(user_id)'
);

-- 两个 INSERT 放进同一个作业，共用一个 Kafka 源
EXECUTE STATEMENT SET
BEGIN
INSERT INTO doris_dws_traffic_category_1m
SELECT
  window_start,
  category_id,
  COUNT(*) FILTER (WHERE behavior = 'pv'),
  COUNT(*) FILTER (WHERE behavior = 'cart'),
  COUNT(*) FILTER (WHERE behavior = 'fav'),
  COUNT(*) FILTER (WHERE behavior = 'buy'),
  COUNT(DISTINCT user_id)
FROM TABLE(TUMBLE(TABLE dwd_user_behavior, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, category_id;
INSERT INTO doris_dws_uv_bitmap_1m
SELECT window_start, category_id, user_id
FROM TABLE(TUMBLE(TABLE dwd_user_behavior, DESCRIPTOR(event_time), INTERVAL '1' MINUTE))
GROUP BY window_start, window_end, category_id, user_id;
END;
