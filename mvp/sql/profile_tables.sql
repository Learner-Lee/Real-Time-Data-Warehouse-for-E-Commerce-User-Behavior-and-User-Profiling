-- 用户画像：用户 × 天的 DWS 表、画像标签表，以及从 ODS 生成 DWS 的批处理 SQL。由 bin/profile.sh 执行，可以重复运行
-- 特征表 dws_user_feature 的列由 profile/features.py 里的特征清单生成（每个特征的中文说明写进列注释），不在这里建
USE r1_dw;

-- DWS：用户 × 天。从 ODS 明细汇总（离线批处理，T+1 的做法）；UNIQUE KEY 保证重跑时覆盖、不重复
CREATE TABLE IF NOT EXISTS dws_user_behavior_1d (
  user_id          BIGINT,
  dt               DATE        COMMENT '事件日期（北京时间）',
  pv_cnt           BIGINT      COMMENT '浏览次数',
  cart_cnt         BIGINT      COMMENT '加购次数',
  fav_cnt          BIGINT      COMMENT '收藏次数',
  buy_cnt          BIGINT      COMMENT '购买行为次数（日志）',
  event_cnt        BIGINT      COMMENT '全部行为次数',
  item_cnt         BIGINT      COMMENT '当天接触的不同商品数',
  category_cnt     BIGINT      COMMENT '当天接触的不同类目数',
  buy_item_cnt     BIGINT      COMMENT '当天购买的不同商品数',
  active_hour_cnt  INT         COMMENT '当天有行为的小时数（0–24）',
  night_event_cnt  BIGINT      COMMENT '0 点到 6 点的行为次数',
  first_event_ts   BIGINT      COMMENT '当天第一条行为的时间（毫秒）',
  last_event_ts    BIGINT      COMMENT '当天最后一条行为的时间（毫秒）',
  last_buy_ts      BIGINT      COMMENT '当天最后一次购买的时间（毫秒），没买为空'
)
UNIQUE KEY (user_id, dt)
DISTRIBUTED BY HASH (user_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- ADS：画像标签，每个用户一行。每次运行整表重写，所以先删再建（表结构改了也能生效）
-- 注意 Doris 的 VARCHAR 长度按字节算，一个汉字占 3 字节
DROP TABLE IF EXISTS ads_user_profile_tag;
CREATE TABLE ads_user_profile_tag (
  user_id           BIGINT,
  profile_dt        DATE           COMMENT '画像基于哪天之前的数据（截止日期，不含当天）',
  active_days       INT            COMMENT '9 天里有行为的天数',
  event_cnt         BIGINT         COMMENT '9 天行为总数',
  r_days            DOUBLE         COMMENT 'R：距最后一次购买的天数，没买过为空',
  f_orders          INT            COMMENT 'F：已支付订单数',
  m_amount          DECIMAL(16,2)  COMMENT 'M：已支付金额（元，模拟价格）',
  r_score           TINYINT        COMMENT 'R 分 1–5，越近越高；没买过为空',
  f_score           TINYINT        COMMENT 'F 分 1–5',
  m_score           TINYINT        COMMENT 'M 分 1–5',
  rfm_class         VARCHAR(64)    COMMENT '经典 RFM 八类，没买过的为「未购买访客」',
  segment_id        TINYINT        COMMENT 'K-Means 分群编号，没买过的为 -1',
  segment_name      VARCHAR(64)    COMMENT '分群名称（按聚类中心自动命名）',
  buy_prob_3d       DOUBLE         COMMENT '未来 3 天购买概率（XGBoost），最近 3 天没活跃为空',
  buy_level_3d      VARCHAR(32)    COMMENT '购买可能性：高 / 中 / 低 / 近 3 天未活跃',
  model_version     VARCHAR(32)    COMMENT '模型版本（训练时间）',
  updated_at        DATETIME       COMMENT '写入时间'
)
UNIQUE KEY (user_id)
DISTRIBUTED BY HASH (user_id) BUCKETS 3
PROPERTIES ("replication_num" = "1");

-- 从 ODS 生成用户 × 天的 DWS（ODS 里包含迟到数据，和离线重算的口径一致）
INSERT INTO dws_user_behavior_1d
SELECT
  user_id,
  DATE(FROM_MILLISECOND(ts))                                        AS dt,
  SUM(IF(behavior = 'pv', 1, 0))                                    AS pv_cnt,
  SUM(IF(behavior = 'cart', 1, 0))                                  AS cart_cnt,
  SUM(IF(behavior = 'fav', 1, 0))                                   AS fav_cnt,
  SUM(IF(behavior = 'buy', 1, 0))                                   AS buy_cnt,
  COUNT(*)                                                          AS event_cnt,
  COUNT(DISTINCT item_id)                                           AS item_cnt,
  COUNT(DISTINCT category_id)                                       AS category_cnt,
  COUNT(DISTINCT IF(behavior = 'buy', item_id, NULL))               AS buy_item_cnt,
  COUNT(DISTINCT HOUR(FROM_MILLISECOND(ts)))                        AS active_hour_cnt,
  SUM(IF(HOUR(FROM_MILLISECOND(ts)) < 6, 1, 0))                     AS night_event_cnt,
  MIN(ts)                                                           AS first_event_ts,
  MAX(ts)                                                           AS last_event_ts,
  MAX(IF(behavior = 'buy', ts, NULL))                               AS last_buy_ts
FROM ods_user_behavior
GROUP BY user_id, DATE(FROM_MILLISECOND(ts));
