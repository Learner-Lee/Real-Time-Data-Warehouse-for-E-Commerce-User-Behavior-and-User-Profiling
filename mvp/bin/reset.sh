#!/usr/bin/env bash
# 清空 R1 的数据，方便重做实验：取消作业、删除 Doris 的 r1_dw 库、删除 R1 的 Kafka topic、清空 MySQL 订单表。
# HBase 维表和 /data/r1 下的回放文件保留。
source "$(dirname "$0")/env.sh"
check_env
check_password

read -r -p "会清空 R1 在 Doris、Kafka、MySQL 里的所有数据（维表和回放文件保留）。确认请输入 yes：" answer
[[ "$answer" == "yes" ]] || die "已取消"

bash "$MVP_DIR/bin/stop_jobs.sh" --with-doris
doris_sql "DROP DATABASE IF EXISTS r1_dw FORCE"
log "已删除 Doris 库 r1_dw"
for topic in ods_user_behavior dwd_user_behavior dws_behavior_1m_stream ods_late_events dwd_catchup; do
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$VM_HOST:9092" --delete --if-exists --topic "$topic"
done
log "已删除 Kafka topic（删除是异步的，等十几秒再运行 setup.sh）"
app_mysql -e "TRUNCATE TABLE orders"
log "已清空 MySQL 订单表。重做实验：bin/setup.sh → bin/start_jobs.sh → bin/replay.sh"
