#!/usr/bin/env bash
# 第 4 步：Doris（FE 和 BE 装在同一台机器上）
source "$(dirname "$0")/lib/common.sh"
check_env

PKG="apache-doris-${DORIS_VERSION}-bin-arm64.tar.gz"
download "$PKG" "$DORIS_DOWNLOAD_BASE/$PKG"
install_tarball "$PKG" "$DORIS_HOME"

FE_CONF="$DORIS_HOME/fe/conf/fe.conf"
BE_CONF="$DORIS_HOME/be/conf/be.conf"
for f in "$FE_CONF" "$BE_CONF"; do
  if [[ ! -f "$f.orig" ]]; then cp "$f" "$f.orig"; fi
done

log "FE：堆内存从默认 8G 改为 $DORIS_FE_HEAP；元数据目录；网段"
sed -E "s/-Xmx[0-9]+[mMgG] -Xms[0-9]+[mMgG]/-Xmx$DORIS_FE_HEAP -Xms$DORIS_FE_HEAP/" "$FE_CONF.orig" > "$FE_CONF"
cat >> "$FE_CONF" <<EOF

# ---------- R1 ----------
priority_networks = $VM_SUBNET
meta_dir = $DATA_DIR/doris/fe-meta
# Routine Load 有积压时，Doris 4 会把每批时长强制拉到至少 360 秒（换吞吐），回放期间数据要 6 分钟才可见。练习环境要的是新鲜度，降到 10 秒
routine_load_adaptive_min_batch_interval_sec = 10
EOF
grep -q -- "-Xmx$DORIS_FE_HEAP" "$FE_CONF" || die "FE 堆内存没有改成功，检查 $FE_CONF 里的 JAVA_OPTS_FOR_JDK_17"

log "BE：内存上限改为 $DORIS_BE_MEM_LIMIT（默认是物理内存的 90%）；JVM 堆 $DORIS_BE_JVM_HEAP；存储目录；网段"
sed -E "s/-Xmx[0-9]+[mMgG]/-Xmx$DORIS_BE_JVM_HEAP/" "$BE_CONF.orig" > "$BE_CONF"
cat >> "$BE_CONF" <<EOF

# ---------- R1 ----------
priority_networks = $VM_SUBNET
storage_root_path = $DATA_DIR/doris/be-storage
mem_limit = $DORIS_BE_MEM_LIMIT
EOF
grep -q -- "-Xmx$DORIS_BE_JVM_HEAP" "$BE_CONF" || die "BE 的 JVM 堆没有改成功，检查 $BE_CONF"
mkdir -p "$DATA_DIR/doris/fe-meta" "$DATA_DIR/doris/be-storage"

log "检查 Doris 对系统的要求"
if [[ $(swapon --show | wc -l) -ne 0 ]]; then die "swap 没有关，先运行 00-base.sh"; fi
if [[ $(sysctl -n vm.max_map_count) -lt 2000000 ]]; then die "vm.max_map_count 不够，先运行 00-base.sh"; fi
if [[ $(ulimit -n) -lt 60000 ]]; then die "文件句柄数只有 $(ulimit -n)：运行 00-base.sh 后要退出并重新登录才生效"; fi

bash "$SCRIPT_DIR/bin/r1-start.sh" doris

log "把 BE 注册到 FE（只需要一次）"
backends=$(doris_sql "SHOW BACKENDS")
if ! grep -q "$VM_IP" <<< "$backends"; then
  doris_sql "ALTER SYSTEM ADD BACKEND '$VM_IP:9050'"
fi

log "等待 BE 心跳正常（最多 2 分钟）"
status=""
for _ in $(seq 1 24); do
  status=$(mysql -h "$VM_HOST" -P 9030 -uroot -e 'SHOW BACKENDS\G' 2>/dev/null || true)
  if grep -q 'Alive: true' <<< "$status"; then break; fi
  sleep 5
done
grep -q 'Alive: true' <<< "$status" || die "BE 没有变成 Alive，看日志 $DORIS_HOME/be/log/be.INFO"
log "BE 已加入集群"

log "验证：建库建表、写一行再查出来"
doris_sql "CREATE DATABASE IF NOT EXISTS r1_install_check"
doris_sql "CREATE TABLE IF NOT EXISTS r1_install_check.t (id INT, name VARCHAR(16)) DUPLICATE KEY(id) DISTRIBUTED BY HASH(id) BUCKETS 1 PROPERTIES ('replication_num' = '1')"
doris_sql "INSERT INTO r1_install_check.t VALUES (1, 'ok')"
echo "查询结果：$(doris_sql "SELECT name FROM r1_install_check.t")（应为 ok）"
doris_sql "DROP DATABASE r1_install_check"
log "完成。网页：http://$VM_IP:8030（用户 root，密码为空）。下一步运行 05-flink.sh"
