#!/usr/bin/env bash
# 启动 R1 组件。用法：r1-start.sh [mysql kafka hbase doris flink grafana]
# 不带参数就按依赖顺序全部启动；已经在运行的组件会跳过；Grafana 是可选的，没装就跳过
source "$(dirname "$0")/../lib/common.sh"

start_mysql() {
  if systemctl is-active --quiet mysql; then
    log "MySQL 已在运行"
    return
  fi
  sudo systemctl start mysql
  wait_port 3306 "MySQL"
}

start_kafka() {
  if pgrep -f 'kafka\.Kafka' >/dev/null; then
    log "Kafka 已在运行"
    return
  fi
  KAFKA_HEAP_OPTS="-Xmx$KAFKA_HEAP -Xms$KAFKA_HEAP" \
    "$KAFKA_HOME/bin/kafka-server-start.sh" -daemon "$KAFKA_HOME/config/server.properties"
  wait_port 9092 "Kafka"
}

start_hbase() {
  if pgrep -f 'hbase\.master\.HMaster' >/dev/null; then
    log "HBase 已在运行"
    return
  fi
  "$HBASE_HOME/bin/start-hbase.sh"
  wait_port 16010 "HBase" 120
}

start_doris() {
  if pgrep -f 'DorisFE' >/dev/null; then
    log "Doris FE 已在运行"
  else
    "$DORIS_HOME/fe/bin/start_fe.sh" --daemon
  fi
  wait_port 9030 "Doris FE" 180
  if pgrep -x 'doris_be' >/dev/null; then
    log "Doris BE 已在运行"
  else
    "$DORIS_HOME/be/bin/start_be.sh" --daemon
  fi
  wait_port 9050 "Doris BE" 120
}

start_flink() {
  if pgrep -f 'StandaloneSessionClusterEntrypoint' >/dev/null; then
    log "Flink 已在运行"
    return
  fi
  "$FLINK_HOME/bin/start-cluster.sh"
  wait_port 8081 "Flink" 90
}

GRAFANA_HOME=${GRAFANA_HOME:-$MODULE_DIR/grafana}
start_grafana() {
  if [[ ! -x "$GRAFANA_HOME/bin/grafana" ]]; then
    log "没装 Grafana（可选，装的话运行 07-grafana.sh），跳过"
    return
  fi
  if pgrep -f 'grafana server' >/dev/null; then
    log "Grafana 已在运行"
    return
  fi
  mkdir -p "$DATA_DIR/grafana/log"
  # setsid + nohup：SSH 断开后继续运行
  setsid nohup "$GRAFANA_HOME/bin/grafana" server --homepath "$GRAFANA_HOME" \
    >"$DATA_DIR/grafana/log/stdout.log" 2>&1 < /dev/null &
  wait_port 3000 "Grafana" 90
}

if [[ $# -eq 0 ]]; then
  set -- mysql kafka hbase doris flink grafana
fi
for component in "$@"; do
  case "$component" in
    mysql | kafka | hbase | doris | flink | grafana) "start_$component" ;;
    *) die "不认识的组件：$component（可选：mysql kafka hbase doris flink grafana）" ;;
  esac
done
