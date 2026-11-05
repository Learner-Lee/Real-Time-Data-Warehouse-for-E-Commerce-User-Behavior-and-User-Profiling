#!/usr/bin/env bash
# 查看 R1 各层的状态：Flink 作业、Kafka 消息数、Doris 各表行数和最新数据时间、Routine Load 状态
source "$(dirname "$0")/env.sh"

log "Flink 作业"
curl -s "http://$VM_HOST:8081/jobs/overview" | jq -r '.jobs[] | "  \(.state)\t\(.name)"' | sort || warn "连不上 Flink"

log "Kafka 各 topic 的最新 offset（事务写入的 topic 会多出事务标记，比实际消息数略大）"
for topic in ods_user_behavior dwd_user_behavior dws_behavior_1m_stream ods_late_events; do
  n=$("$KAFKA_HOME/bin/kafka-get-offsets.sh" --bootstrap-server "$VM_HOST:9092" --topic "$topic" 2>/dev/null |
      awk -F: '{ s += $3 } END { print s + 0 }')
  printf '  %-24s %s\n' "$topic" "$n"
done

log "Doris 各表：行数 / 最新数据时间"
doris_sql "
  SELECT 'ods_user_behavior', COUNT(*), FROM_MILLISECOND(MAX(ts)) FROM r1_dw.ods_user_behavior
  UNION ALL SELECT 'dws_traffic_category_1m', COUNT(*), MAX(window_start) FROM r1_dw.dws_traffic_category_1m
  UNION ALL SELECT 'dws_uv_bitmap_1m', COUNT(*), MAX(window_start) FROM r1_dw.dws_uv_bitmap_1m
  UNION ALL SELECT 'dws_trade_1m', COUNT(*), MAX(minute_start) FROM r1_dw.dws_trade_1m
  UNION ALL SELECT 'dws_behavior_1m_stream', COUNT(*), MAX(window_start) FROM r1_dw.dws_behavior_1m_stream
  UNION ALL SELECT 'dwd_late_events', COUNT(*), FROM_MILLISECOND(MAX(ts)) FROM r1_dw.dwd_late_events
" | awk -F'\t' '{ printf "  %-24s %12s   %s\n", $1, $2, $3 }'

log "Routine Load 状态（Lag = 每个分区还没导入的条数）"
# Doris 4 不支持 SHOW ROUTINE LOAD FROM 库名，要先选库再 SHOW
mysql -h "$VM_HOST" -P 9030 -uroot r1_dw -e 'SHOW ROUTINE LOAD\G' |
  grep -E '^ *(Name|State|Lag|ReasonOfStateChanged):' | sed 's/^ */  /'

log "大屏查询和对账 SQL 在 sql/ads_dashboard.sql"
