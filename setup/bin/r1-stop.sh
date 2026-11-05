#!/usr/bin/env bash
# 停止 R1 组件。用法：r1-stop.sh [flink doris hbase kafka mysql]
# 不带参数就按启动的相反顺序停掉除 MySQL 以外的全部组件（MySQL 占内存少，默认一直开着）
source "$(dirname "$0")/../lib/common.sh"

stop_flink() { "$FLINK_HOME/bin/stop-cluster.sh" || true; }

stop_doris() {
  "$DORIS_HOME/be/bin/stop_be.sh" || true
  "$DORIS_HOME/fe/bin/stop_fe.sh" || true
}

stop_hbase() { "$HBASE_HOME/bin/stop-hbase.sh" || true; }

stop_kafka() {
  "$KAFKA_HOME/bin/kafka-server-stop.sh" || true
  # 等 Kafka 刷完盘正常退出，否则下次启动要做日志恢复
  for _ in $(seq 1 30); do
    pgrep -f 'kafka\.Kafka' >/dev/null || return 0
    sleep 1
  done
  warn "Kafka 30 秒内没有退出，可以用 r1-status.sh 看一下"
}

stop_mysql() { sudo systemctl stop mysql; }

if [[ $# -eq 0 ]]; then
  set -- flink doris hbase kafka
fi
for component in "$@"; do
  case "$component" in
    mysql | kafka | hbase | doris | flink)
      log "停止 $component"
      "stop_$component"
      ;;
    *) die "不认识的组件：$component（可选：flink doris hbase kafka mysql）" ;;
  esac
done
