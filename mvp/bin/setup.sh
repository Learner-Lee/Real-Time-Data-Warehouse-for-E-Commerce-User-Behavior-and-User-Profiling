#!/usr/bin/env bash
# 一次性准备：建 Kafka topic、HBase 维表、MySQL 订单表、Doris 表，并把维表导进 HBase。可以重复运行
source "$(dirname "$0")/env.sh"
check_env
check_password

for f in behavior_sorted.csv dim_user.csv dim_item.csv; do
  [[ -s "$R1_DATA/$f" ]] || die "缺少 $R1_DATA/$f：先运行 bin/prepare.sh"
done

log "0/5 确认组件都在运行"
bash "$R1_SETUP/bin/r1-start.sh"

log "1/5 建 Kafka topic"
create_topic ods_user_behavior 3        # 回放器写入的原始行为
create_topic dwd_user_behavior 3        # Flink SQL 清洗、关联维表后的明细
create_topic dws_behavior_1m_stream 1   # DataStream 作业的窗口结果
create_topic ods_late_events 1          # DataStream 作业的侧输出（超过允许迟到时间的数据）

log "2/5 建 HBase 维表（加盐 rowkey + 预分区）"
"$HBASE_HOME/bin/hbase" shell -n < "$MVP_DIR/sql/hbase_tables.rb" > "$WORK_DIR/hbase_tables.log" 2>&1 ||
  die "建 HBase 表失败，看 $WORK_DIR/hbase_tables.log"
grep -E '已存在|已创建' "$WORK_DIR/hbase_tables.log" || true

log "3/5 建 MySQL 订单表"
app_mysql < "$MVP_DIR/sql/mysql_orders.sql"

log "4/5 建 Doris 表"
mysql -h "$VM_HOST" -P 9030 -uroot < "$MVP_DIR/sql/doris_tables.sql"

log "5/5 维表导入 HBase（Flink 作业，读完文件自动结束）"
run_flink_sql 00_load_dims.sql

log "检查 HBase 维表行数（应和 dim_user.csv、dim_item.csv 的行数一致）"
for table in dim_user dim_item; do
  rows=$(echo "count '$table', INTERVAL => 1000000" | "$HBASE_HOME/bin/hbase" shell -n 2>/dev/null | grep -oE '^[0-9]+ row' | head -1 || true)
  csv_rows=$(wc -l < "$R1_DATA/$table.csv")
  echo "  $table：HBase ${rows:-0 row}(s)，CSV $csv_rows 行"
done
log "完成。下一步：在 Mac 上打好 Java 作业的 jar 并复制过来，然后运行 bin/start_jobs.sh"
