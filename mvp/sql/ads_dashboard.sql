-- R1 大屏查询与对账。在 Doris 里执行：mysql -h bigdata01 -P 9030 -uroot r1_dw，然后逐条复制运行
USE r1_dw;

-- ============ 一、大屏指标 ============

-- 1. 每天的 PV、UV、加购、收藏、购买。UV 用分钟级 BITMAP 合并，跨任意时间段都是精确去重
SELECT t.dt, t.pv, u.uv, t.cart_cnt, t.fav_cnt, t.buy_cnt
FROM (
  SELECT DATE(window_start) AS dt, SUM(pv) AS pv, SUM(cart_cnt) AS cart_cnt, SUM(fav_cnt) AS fav_cnt, SUM(buy_cnt) AS buy_cnt
  FROM dws_traffic_category_1m
  GROUP BY DATE(window_start)
) t
JOIN (
  SELECT DATE(window_start) AS dt, BITMAP_UNION_COUNT(user_id) AS uv
  FROM dws_uv_bitmap_1m
  GROUP BY DATE(window_start)
) u ON t.dt = u.dt
ORDER BY t.dt;

-- 2. 转化漏斗（按天）：浏览 → 加购 → 购买
SELECT DATE(window_start) AS dt,
       SUM(pv) AS pv,
       SUM(cart_cnt) AS cart,
       SUM(buy_cnt) AS buy,
       ROUND(SUM(cart_cnt) * 100.0 / SUM(pv), 2) AS pv_to_cart_pct,
       ROUND(SUM(buy_cnt) * 100.0 / NULLIF(SUM(cart_cnt), 0), 2) AS cart_to_buy_pct
FROM dws_traffic_category_1m
GROUP BY DATE(window_start)
ORDER BY dt;

-- 3. 每小时 GMV（来自 MySQL CDC 订单；退款发生后，对应时段的数字会回撤）
SELECT HOUR_FLOOR(minute_start) AS hour_start,
       SUM(order_cnt) AS orders,
       SUM(gmv) AS gmv,
       SUM(refund_cnt) AS refunds
FROM dws_trade_1m
GROUP BY HOUR_FLOOR(minute_start)
ORDER BY hour_start DESC
LIMIT 24;

-- 4. 购买次数最多的 10 个类目
SELECT category_id, SUM(pv) AS pv, SUM(buy_cnt) AS buy, ROUND(SUM(buy_cnt) * 100.0 / NULLIF(SUM(pv), 0), 3) AS buy_rate_pct
FROM dws_traffic_category_1m
GROUP BY category_id
ORDER BY buy DESC
LIMIT 10;

-- 5. 数据新鲜度：各层最新数据的时间（回放进行中时，越接近当前回放进度越好）
SELECT 'ods_user_behavior' AS layer, FROM_MILLISECOND(MAX(ts)) AS latest FROM ods_user_behavior
UNION ALL SELECT 'dws_traffic_category_1m', MAX(window_start) FROM dws_traffic_category_1m
UNION ALL SELECT 'dws_trade_1m', MAX(minute_start) FROM dws_trade_1m
UNION ALL SELECT 'dws_behavior_1m_stream', MAX(window_start) FROM dws_behavior_1m_stream;

-- ============ 二、对账（回放全部结束、作业都跑完后再看）============
-- 回放结束后，没有新数据推动 Watermark，最后一两个窗口不会触发，这是流处理的正常现象。
-- 所以下面的对账都排除最后 3 分钟（cutoff）。

-- 6. 对账 A：Flink SQL 窗口（迟到数据直接丢弃） vs 离线重算（ODS 明细，包含迟到数据）
--    diff 就是被 SQL 窗口丢掉的迟到数据条数；只列出有差异的分钟。
--    注意：上游 DWD 用事务写入，下游每 30 秒才看到一批（约 5 小时事件时间），而 Watermark 每 200 毫秒才更新一次，
--    同一批里的迟到数据在 Watermark 越过窗口前就处理完了，所以实测 diff 远小于注入的迟到条数（2026-11-03：74 条 vs 6,652 条）
WITH cutoff AS (
  SELECT MINUTE_FLOOR(FROM_MILLISECOND(MAX(ts))) - INTERVAL 3 MINUTE AS t FROM ods_user_behavior
)
SELECT o.minute_start, o.pv AS pv_offline, COALESCE(s.pv, 0) AS pv_sql, o.pv - COALESCE(s.pv, 0) AS diff
FROM (
  SELECT MINUTE_FLOOR(FROM_MILLISECOND(ts)) AS minute_start, COUNT(*) AS pv
  FROM ods_user_behavior
  WHERE behavior = 'pv'
  GROUP BY MINUTE_FLOOR(FROM_MILLISECOND(ts))
) o
LEFT JOIN (
  SELECT window_start AS minute_start, SUM(pv) AS pv
  FROM dws_traffic_category_1m
  GROUP BY window_start
) s ON o.minute_start = s.minute_start
WHERE o.pv <> COALESCE(s.pv, 0)
  AND o.minute_start < (SELECT t FROM cutoff)
ORDER BY o.minute_start
LIMIT 20;

-- 7. 对账 B：DataStream 作业（允许迟到 1 分钟 + 侧输出）三方核对，按分钟列出
--    实时结果 + 侧输出里的迟到条数 = 离线重算，unexplained 应该全部为 0，说明一条数据都没丢
WITH cutoff AS (
  SELECT MINUTE_FLOOR(FROM_MILLISECOND(MAX(ts))) - INTERVAL 3 MINUTE AS t FROM ods_user_behavior
)
SELECT o.minute_start,
       o.cnt AS offline_cnt,
       COALESCE(s.cnt, 0) AS stream_cnt,
       COALESCE(l.cnt, 0) AS late_cnt,
       o.cnt - COALESCE(s.cnt, 0) - COALESCE(l.cnt, 0) AS unexplained
FROM (
  SELECT MINUTE_FLOOR(FROM_MILLISECOND(ts)) AS minute_start, COUNT(*) AS cnt
  FROM ods_user_behavior
  WHERE behavior = 'pv'
  GROUP BY MINUTE_FLOOR(FROM_MILLISECOND(ts))
) o
LEFT JOIN (
  SELECT window_start AS minute_start, cnt
  FROM dws_behavior_1m_stream
  WHERE behavior = 'pv'
) s ON o.minute_start = s.minute_start
LEFT JOIN (
  SELECT MINUTE_FLOOR(FROM_MILLISECOND(ts)) AS minute_start, COUNT(*) AS cnt
  FROM dwd_late_events
  WHERE behavior = 'pv'
  GROUP BY MINUTE_FLOOR(FROM_MILLISECOND(ts))
) l ON o.minute_start = l.minute_start
WHERE o.cnt <> COALESCE(s.cnt, 0)
  AND o.minute_start < (SELECT t FROM cutoff)
ORDER BY o.minute_start
LIMIT 20;

-- 8. 对账 B 的汇总：全部分钟加起来（不含最后 3 分钟），unexplained 应该为 0
WITH cutoff AS (
  SELECT MINUTE_FLOOR(FROM_MILLISECOND(MAX(ts))) - INTERVAL 3 MINUTE AS t FROM ods_user_behavior
)
SELECT SUM(o.cnt) AS offline_total,
       SUM(COALESCE(s.cnt, 0)) AS stream_total,
       SUM(COALESCE(l.cnt, 0)) AS late_total,
       SUM(o.cnt) - SUM(COALESCE(s.cnt, 0)) - SUM(COALESCE(l.cnt, 0)) AS unexplained
FROM (
  SELECT MINUTE_FLOOR(FROM_MILLISECOND(ts)) AS minute_start, COUNT(*) AS cnt
  FROM ods_user_behavior
  WHERE behavior = 'pv'
  GROUP BY MINUTE_FLOOR(FROM_MILLISECOND(ts))
) o
LEFT JOIN (
  SELECT window_start AS minute_start, cnt FROM dws_behavior_1m_stream WHERE behavior = 'pv'
) s ON o.minute_start = s.minute_start
LEFT JOIN (
  SELECT MINUTE_FLOOR(FROM_MILLISECOND(ts)) AS minute_start, COUNT(*) AS cnt
  FROM dwd_late_events
  WHERE behavior = 'pv'
  GROUP BY MINUTE_FLOOR(FROM_MILLISECOND(ts))
) l ON o.minute_start = l.minute_start
WHERE o.minute_start < (SELECT t FROM cutoff);

-- 9. 回放器注入的迟到数据去了哪里：按注入的迟到秒数，对比 ODS 里的条数和侧输出里的条数
--    迟到 30 秒的在允许迟到范围内，应该几乎都不进侧输出。迟到 180 秒的只有一部分进侧输出（2026-11-05 实测 2,890 / 4,946）：
--    600 倍速回放时，Watermark 每 200 毫秒更新一次，相当于事件时间 2 分钟一跳；再加上 3 个分区之间的读取进度差，
--    很多迟到数据到达时 Watermark 还没越过「窗口结束 + 允许迟到 1 分钟」，就被正常计入窗口。按 1 倍速运行时会基本全部进侧输出。
--    注意这里统计全部行为，第 8 项只统计 pv，所以两边的条数不同
SELECT o.late_delay, o.injected, COALESCE(l.side_output, 0) AS side_output
FROM (
  SELECT late_delay, COUNT(*) AS injected FROM ods_user_behavior WHERE late_delay IS NOT NULL GROUP BY late_delay
) o
LEFT JOIN (
  SELECT late_delay, COUNT(*) AS side_output FROM dwd_late_events WHERE late_delay IS NOT NULL GROUP BY late_delay
) l ON o.late_delay = l.late_delay
ORDER BY o.late_delay;

-- 10. GMV 对账：Flink 实时汇总 vs 直接在 MySQL 里算（在 MySQL 里执行下面这句，和本查询的结果比较）
--     MySQL：SELECT COUNT(*), SUM(amount) FROM r1_shop.orders WHERE status = 'PAID';
SELECT SUM(order_cnt) AS paid_orders, SUM(gmv) AS gmv FROM dws_trade_1m;
