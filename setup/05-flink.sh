#!/usr/bin/env bash
# 第 5 步：Flink 1.20（standalone 模式）+ R1 要用的 4 个连接器、MySQL 驱动和 Hadoop 客户端
source "$(dirname "$0")/lib/common.sh"
check_env

PKG="flink-${FLINK_VERSION}-bin-scala_2.12.tgz"
# shellcheck disable=SC2046
download "$PKG" $(apache_urls "flink/flink-$FLINK_VERSION/$PKG")
install_tarball "$PKG" "$FLINK_HOME"

log "下载连接器和依赖，放进 $FLINK_HOME/lib/"
# Hadoop 客户端：HBase 连接器运行时要用 Hadoop 的类（如 org.apache.hadoop.io.Writable），缺了会报 NoClassDefFoundError。
# 用官方打包好的 hadoop-client-api / runtime（第三方依赖都重定位过），不会和 Flink 自己的依赖冲突
jars=(
  "org.apache.flink flink-sql-connector-kafka $FLINK_KAFKA_CONNECTOR_VERSION"
  "org.apache.flink flink-sql-connector-mysql-cdc $FLINK_MYSQL_CDC_VERSION"
  "org.apache.flink flink-sql-connector-hbase-2.2 $FLINK_HBASE_CONNECTOR_VERSION"
  "org.apache.doris flink-doris-connector-1.20 $FLINK_DORIS_CONNECTOR_VERSION"
  "com.mysql mysql-connector-j $MYSQL_JDBC_VERSION"
  "org.apache.hadoop hadoop-client-api $HADOOP_CLIENT_VERSION"
  "org.apache.hadoop hadoop-client-runtime $HADOOP_CLIENT_VERSION"
)
for spec in "${jars[@]}"; do
  read -r group artifact version <<< "$spec"
  jar="$artifact-$version.jar"
  # shellcheck disable=SC2046
  download "$jar" $(maven_urls "$group" "$artifact" "$version")
  cp "$SOFT_DIR/$jar" "$FLINK_HOME/lib/"
done

# HBase 连接器里打包的是 ZooKeeper 3.4.14 客户端，它在 JDK 14+ 上解析不了主机名（ZOOKEEPER-3779），
# 表现为 "bigdata01/<unresolved>:2181" 一直重连、Lookup Join 报 ConnectionLoss。连接器没有更新的版本，
# 所以编译 patches/ 里改过的 StaticHostProvider，替换进刚复制的 jar（每次运行都从原始 jar 重新打）
log "给 HBase 连接器打补丁：修复 ZooKeeper 3.4 客户端在 JDK 17 上解析不了主机名"
HBASE_CONNECTOR_JAR="$FLINK_HOME/lib/flink-sql-connector-hbase-2.2-$FLINK_HBASE_CONNECTOR_VERSION.jar"
PATCH_BUILD=$(mktemp -d)
javac -nowarn --release 8 -cp "$FLINK_HOME/lib/*" -d "$PATCH_BUILD" \
  "$SCRIPT_DIR/patches/zk-3.4-jdk14/StaticHostProvider.java" 2>&1 | grep -v '^warning' || true
[[ -f "$PATCH_BUILD/org/apache/flink/hbase/shaded/org/apache/zookeeper/client/StaticHostProvider.class" ]] ||
  die "补丁编译失败（需要 JDK 的 javac）"
(cd "$PATCH_BUILD" && jar uf "$HBASE_CONNECTOR_JAR" org/apache/flink/hbase/shaded/org/apache/zookeeper/client/*.class)
rm -rf "$PATCH_BUILD"

CONF="$FLINK_HOME/conf/config.yaml"
log "改 config.yaml（原文件备份为 config.yaml.orig，每次都从备份重新生成）"
if [[ ! -f "$CONF.orig" ]]; then cp "$CONF" "$CONF.orig"; fi
cp "$CONF.orig" "$CONF"
python3 "$SCRIPT_DIR/lib/set_yaml.py" "$CONF" \
  jobmanager.rpc.address="$VM_HOST" \
  jobmanager.bind-host=0.0.0.0 \
  jobmanager.memory.process.size="$FLINK_JM_MEMORY" \
  taskmanager.host="$VM_HOST" \
  taskmanager.bind-host=0.0.0.0 \
  taskmanager.memory.process.size="$FLINK_TM_MEMORY" \
  taskmanager.numberOfTaskSlots="$FLINK_TM_SLOTS" \
  parallelism.default=2 \
  rest.address="$VM_HOST" \
  rest.bind-address=0.0.0.0 \
  state.backend.type=rocksdb \
  state.backend.incremental=true \
  state.checkpoints.dir="file://$DATA_DIR/flink/checkpoints" \
  state.savepoints.dir="file://$DATA_DIR/flink/savepoints"
mkdir -p "$DATA_DIR/flink/checkpoints" "$DATA_DIR/flink/savepoints"

log "（重新）启动 Flink，让配置生效"
bash "$SCRIPT_DIR/bin/r1-stop.sh" flink
bash "$SCRIPT_DIR/bin/r1-start.sh" flink

log "验证：等 TaskManager 注册上来"
for _ in $(seq 1 30); do
  overview=$(curl -s "http://$VM_HOST:8081/overview" || true)
  if [[ $(jq -r '.taskmanagers // 0' <<< "$overview" 2>/dev/null) -ge 1 ]]; then
    jq '{"Flink 版本": ."flink-version", "TaskManager 数": .taskmanagers, "slot 总数": ."slots-total"}' <<< "$overview"
    log "完成。网页：http://$VM_IP:8081。下一步运行 06-python.sh"
    exit 0
  fi
  sleep 2
done
die "TaskManager 没有注册上来，看日志 $FLINK_HOME/log"
