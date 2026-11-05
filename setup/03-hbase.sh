#!/usr/bin/env bash
# 第 3 步：HBase（单机模式，自带 ZooKeeper，数据存本地磁盘）
source "$(dirname "$0")/lib/common.sh"
check_env

PKG="hbase-${HBASE_VERSION}-hadoop3-bin.tar.gz"
# shellcheck disable=SC2046
download "$PKG" $(apache_urls "hbase/$HBASE_VERSION/$PKG")
install_tarball "$PKG" "$HBASE_HOME"

CONF_DIR="$HBASE_HOME/conf"
log "写配置 hbase-site.xml（原文件备份为 hbase-site.xml.orig）"
if [[ ! -f "$CONF_DIR/hbase-site.xml.orig" ]]; then cp "$CONF_DIR/hbase-site.xml" "$CONF_DIR/hbase-site.xml.orig"; fi
sed -e "s|@VM_HOST@|$VM_HOST|g" -e "s|@DATA_DIR@|$DATA_DIR|g" "$SCRIPT_DIR/conf/hbase-site.xml" > "$CONF_DIR/hbase-site.xml"

log "在 hbase-env.sh 末尾追加 R1 设置（JDK 17 需要的启动参数，HBase 2.6 的启动脚本会自己加）"
if ! grep -q '# ---------- R1 ----------' "$CONF_DIR/hbase-env.sh"; then
  cat >> "$CONF_DIR/hbase-env.sh" <<EOF

# ---------- R1 ----------
export JAVA_HOME=$JAVA_HOME
export HBASE_HEAPSIZE=$HBASE_HEAP
export HBASE_MANAGES_ZK=true
export HBASE_LOG_DIR=$DATA_DIR/hbase/logs
export HBASE_PID_DIR=$DATA_DIR/hbase/pids
export HBASE_DISABLE_HADOOP_CLASSPATH_LOOKUP=true
EOF
fi
mkdir -p "$DATA_DIR/hbase/logs" "$DATA_DIR/hbase/pids"

bash "$SCRIPT_DIR/bin/r1-start.sh" hbase

log "验证：执行 status（HBase 刚启动时要初始化，最多等 1 分钟）"
for _ in 1 2 3 4 5 6; do
  out=$(echo "status" | "$HBASE_HOME/bin/hbase" shell -n 2>/dev/null || true)
  if grep -q "active master" <<< "$out"; then
    grep "active master" <<< "$out"
    log "完成。网页：http://$VM_IP:16010。下一步运行 04-doris.sh"
    exit 0
  fi
  sleep 10
done
die "HBase 没有正常起来，看日志 $DATA_DIR/hbase/logs"
