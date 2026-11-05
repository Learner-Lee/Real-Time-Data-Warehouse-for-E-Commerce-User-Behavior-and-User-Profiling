#!/usr/bin/env bash
# 检查 ODS 和 DWD 每个分区内部是否按事件时间有序，以及两边的分区是否一一对应（同一用户在同一个分区号）。
# 下游窗口按分区算 Watermark（允许乱序 5 秒），分区内落后超过 5 秒的数据会被当成迟到丢弃，所以这两项决定了流量指标准不准。
# 用法：bash bin/check_order.sh [每个分区最多读多少条，默认全部]
#      DWD_TOPIC=dwd_catchup bash bin/check_order.sh   检查补数据实验写出的 topic
source "$(dirname "$0")/env.sh"
MAX=${1:-1000000}
DWD_TOPIC=${DWD_TOPIC:-dwd_user_behavior}

dump_partition() {   # 用法：dump_partition topic 分区号；输出 "user_id ts"，只读已提交的消息
  "$KAFKA_HOME/bin/kafka-console-consumer.sh" --bootstrap-server "$VM_HOST:9092" --topic "$1" --partition "$2" \
    --offset earliest --max-messages "$MAX" --timeout-ms 15000 --isolation-level read_committed 2>/dev/null |
    jq -r '"\(.user_id) \(.ts)"'
}

log "每个分区：落后当前最大事件时间超过 5 秒的条数（ODS 里只应有回放器故意注入的约 1.5% 迟到数据）"
for topic in ods_user_behavior "$DWD_TOPIC"; do
  for p in 0 1 2; do
    dump_partition "$topic" "$p" > "$WORK_DIR/order_${topic}_$p.txt"
    awk -v name="$topic/$p" '
      { n++; if ($2 < mx - 5000) { late++; if (mx - $2 > worst) worst = mx - $2 } if ($2 > mx) mx = $2 }
      END { printf "  %-22s %8d 条，落后超过 5 秒 %6d 条（%5.2f%%），最多落后 %.1f 分钟\n",
                   name, n, late, (n ? 100 * late / n : 0), worst / 60000 }' "$WORK_DIR/order_${topic}_$p.txt"
  done
done

log "分区是否一一对应：$DWD_TOPIC 分区 p 里的用户，是否都来自 ODS 分区 p"
for p in 0 1 2; do
  cut -d' ' -f1 "$WORK_DIR/order_ods_user_behavior_$p.txt" | sort -u > "$WORK_DIR/users_ods_$p.txt"
  cut -d' ' -f1 "$WORK_DIR/order_${DWD_TOPIC}_$p.txt" | sort -u > "$WORK_DIR/users_dwd_$p.txt"
  extra=$(comm -13 "$WORK_DIR/users_ods_$p.txt" "$WORK_DIR/users_dwd_$p.txt" | wc -l)
  printf '  分区 %s：ODS 用户 %5d 个，DWD 用户 %5d 个，DWD 里不属于对应 ODS 分区的用户 %d 个\n' \
    "$p" "$(wc -l < "$WORK_DIR/users_ods_$p.txt")" "$(wc -l < "$WORK_DIR/users_dwd_$p.txt")" "$extra"
done
