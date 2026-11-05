#!/usr/bin/env bash
# 第 2 步：Kafka（KRaft 模式，单节点同时担任 broker 和 controller，不需要 ZooKeeper）
source "$(dirname "$0")/lib/common.sh"
check_env

PKG="kafka_${KAFKA_SCALA_VERSION}-${KAFKA_VERSION}.tgz"
# shellcheck disable=SC2046
download "$PKG" $(apache_urls "kafka/$KAFKA_VERSION/$PKG")
install_tarball "$PKG" "$KAFKA_HOME"

CONF="$KAFKA_HOME/config/server.properties"
log "写配置 $CONF（原文件备份为 server.properties.orig）"
if [[ ! -f "$CONF.orig" ]]; then cp "$CONF" "$CONF.orig"; fi
sed -e "s|@VM_HOST@|$VM_HOST|g" -e "s|@DATA_DIR@|$DATA_DIR|g" "$SCRIPT_DIR/conf/kafka-server.properties" > "$CONF"

log "格式化存储目录（只在第一次执行）"
mkdir -p "$DATA_DIR/kafka/logs"
if [[ ! -f "$DATA_DIR/kafka/logs/meta.properties" ]]; then
  cluster_id=$("$KAFKA_HOME/bin/kafka-storage.sh" random-uuid)
  "$KAFKA_HOME/bin/kafka-storage.sh" format --standalone -t "$cluster_id" -c "$CONF"
fi

bash "$SCRIPT_DIR/bin/r1-start.sh" kafka

log "验证：建一个测试 topic，再删掉"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$VM_HOST:9092" --create --if-not-exists \
  --topic r1_install_check --partitions 1 --replication-factor 1
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$VM_HOST:9092" --delete --topic r1_install_check
log "完成。下一步运行 03-hbase.sh"
