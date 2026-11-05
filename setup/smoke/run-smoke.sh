#!/usr/bin/env bash
# R1 环境冒烟测试：验证三条链路都能把数据写进 Doris
#   1. Kafka → Flink SQL → Doris
#   2. MySQL CDC（全量 + 增量）→ Flink SQL → Doris
#   3. Kafka 关联 HBase 维表（异步 Lookup Join）→ Doris
# 可以重复运行：每次都会重建测试用的表和数据，结束时取消测试作业、释放 slot。
source "$(dirname "$0")/../lib/common.sh"
check_env
check_password

SMOKE_DIR="$SCRIPT_DIR/smoke"
WORK_DIR="$DATA_DIR/smoke"              # 生成的 SQL 里有密码，只放在虚拟机上，不进 git
LABEL_SUFFIX=$(date +%Y%m%d%H%M%S)      # Doris 的导入 label 不能重复，每次运行换一个后缀
mkdir -p "$WORK_DIR"

# 同时只能跑一份：两份并发会各提交 3 个作业，4 个 slot 不够，后提交的 HBase 作业拿不到 slot，第 3 项失败（2026-11-04 实测）
exec 9> "$WORK_DIR/.lock"
flock -n 9 || die "已经有一份冒烟测试在运行（可能在另一个 SSH 窗口里），等它结束再跑"

app_mysql() { MYSQL_PWD="$MYSQL_APP_PASSWORD" mysql -h "$VM_HOST" -u"$MYSQL_APP_USER" r1_shop "$@"; }

# 把 SQL 模板里的 @...@ 换成实际值，生成到 $WORK_DIR，返回生成文件的路径
render() {
  sed -e "s|@VM_HOST@|$VM_HOST|g" \
      -e "s|@MYSQL_APP_USER@|$MYSQL_APP_USER|g" \
      -e "s|@MYSQL_APP_PASSWORD@|$MYSQL_APP_PASSWORD|g" \
      -e "s|@LABEL_SUFFIX@|$LABEL_SUFFIX|g" \
      "$SMOKE_DIR/$1" > "$WORK_DIR/$1"
  echo "$WORK_DIR/$1"
}

# 反复查 Doris，直到结果等于期望值，最多等 2 分钟
expect_doris() {   # 用法：expect_doris 描述 SQL 期望值
  local desc=$1 sql=$2 expect=$3 got="" i
  for ((i = 0; i < 24; i++)); do
    got=$(doris_sql "$sql" 2>/dev/null || true)
    if [[ "$got" == "$expect" ]]; then
      printf '\033[32m  通过\033[0m %s\n' "$desc"
      return 0
    fi
    sleep 5
  done
  printf '\033[31m  失败\033[0m %s：期望 %s，实际 %s\n' "$desc" "$expect" "${got:-空}"
  return 1
}

cancel_smoke_jobs() {
  local job
  for job in $("$FLINK_HOME/bin/flink" list -r 2>/dev/null | awk -F' : ' '/smoke-/ { print $2 }'); do
    if "$FLINK_HOME/bin/flink" cancel "$job" >/dev/null 2>&1; then
      log "已取消作业 $job"
    else
      warn "取消作业 $job 失败"
    fi
  done
}

log "0/5 确认组件都在运行，并取消上次没结束的冒烟作业"
bash "$SCRIPT_DIR/bin/r1-start.sh"
cancel_smoke_jobs
# 3 个测试作业各占 1 个 slot。slot 被别的作业占着时，作业会一直重启、等不到资源，最后表现成「结果为空」，不如提前说清楚
free_slots=$(curl -s "http://$VM_HOST:8081/overview" | jq -r '."slots-available"')
[[ "$free_slots" =~ ^[0-9]+$ && $free_slots -ge 3 ]] ||
  die "Flink 空闲 slot 只有 ${free_slots:-?} 个，冒烟测试要 3 个。先停掉占用 slot 的作业（R1 MVP 用 ~/r1-realtime-dw/bin/stop_jobs.sh）"

log "1/5 建 Doris 结果表"
mysql -h "$VM_HOST" -P 9030 -uroot < "$SMOKE_DIR/doris-tables.sql"

log "2/5 往 Kafka 写 3 条测试消息"
"$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$VM_HOST:9092" --create --if-not-exists \
  --topic smoke_events --partitions 1 --replication-factor 1
printf '%s\n' \
  '{"id":1,"user_id":"u1","action":"pv","ts":1700000000000}' \
  '{"id":2,"user_id":"u2","action":"cart","ts":1700000001000}' \
  '{"id":3,"user_id":"u3","action":"buy","ts":1700000002000}' |
  "$KAFKA_HOME/bin/kafka-console-producer.sh" --bootstrap-server "$VM_HOST:9092" --topic smoke_events

log "3/5 准备 MySQL 订单表（先放 2 行）和 HBase 维表（u1=gold、u2=silver）"
app_mysql <<'EOF'
DROP TABLE IF EXISTS smoke_orders;
CREATE TABLE smoke_orders (
  order_id    BIGINT PRIMARY KEY,
  user_id     VARCHAR(32),
  amount      DECIMAL(10, 2),
  update_time DATETIME(3) DEFAULT CURRENT_TIMESTAMP(3) ON UPDATE CURRENT_TIMESTAMP(3)
);
INSERT INTO smoke_orders (order_id, user_id, amount) VALUES (101, 'u1', 99.90), (102, 'u2', 15.50);
EOF
"$HBASE_HOME/bin/hbase" shell -n > "$WORK_DIR/hbase-shell.log" 2>&1 <<'EOF' || die "HBase 准备数据失败，看 $WORK_DIR/hbase-shell.log"
if list.include?('smoke_dim') then disable 'smoke_dim'; drop 'smoke_dim' end
create 'smoke_dim', 'f'
put 'smoke_dim', 'u1', 'f:level', 'gold'
put 'smoke_dim', 'u2', 'f:level', 'silver'
EOF

log "4/5 提交 3 个 Flink SQL 作业"
for sql in kafka-to-doris.sql mysql-cdc-to-doris.sql hbase-lookup-to-doris.sql; do
  "$FLINK_HOME/bin/sql-client.sh" -f "$(render "$sql")" > "$WORK_DIR/${sql%.sql}.log" 2>&1 ||
    die "提交 $sql 失败，看日志 $WORK_DIR/${sql%.sql}.log"
done
log "  等 30 秒让 CDC 读完全量，再往 MySQL 插 1 行，验证增量阶段"
sleep 30
app_mysql -e "INSERT INTO smoke_orders (order_id, user_id, amount) VALUES (103, 'u3', 8.00);"

log "5/5 检查 Doris 里的结果（每项最多等 2 分钟）"
failed=0
expect_doris "Kafka → Flink SQL → Doris" \
  "SELECT COUNT(*) FROM r1_smoke.kafka_events" "3" || failed=1
expect_doris "MySQL CDC（全量 2 行 + 增量 1 行）→ Doris" \
  "SELECT COUNT(*) FROM r1_smoke.mysql_orders" "3" || failed=1
expect_doris "Kafka 关联 HBase 维表（异步 Lookup Join）→ Doris" \
  "SELECT level FROM r1_smoke.enriched_events WHERE id = 1" "gold" || failed=1

cancel_smoke_jobs
if [[ $failed -eq 0 ]]; then
  log "全部通过：R1 环境可用"
else
  die "有检查没通过。作业报错看 http://$VM_IP:8081 的 Exceptions 页，提交日志在 $WORK_DIR"
fi
