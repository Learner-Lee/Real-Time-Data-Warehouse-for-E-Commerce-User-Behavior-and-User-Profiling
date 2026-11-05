#!/usr/bin/env bash
# 提交 R1 的 4 个 Flink 作业，并启动 Doris Routine Load。
# 单节点 4 个 slot：DWD、DWS 流量、DWS 交易、迟到数据作业各占 1 个（先运行冒烟测试的话，它结束时会释放自己的 slot）
source "$(dirname "$0")/env.sh"
check_env
check_password

JAR="$MVP_DIR/late-data-job/target/r1-late-data-job.jar"
[[ -f "$JAR" ]] || die "找不到 $JAR：先在 Mac 上打包（见 README 第三节），再把整个目录复制过来"

running=$(running_r1_jobs)
if [[ -n "$running" ]]; then
  die "已经有 R1 作业在运行，先运行 bin/stop_jobs.sh，避免重复提交"
fi

log "1/3 提交 Flink SQL 作业：DWD、DWS 流量、DWS 交易"
for file in 10_dwd_user_behavior.sql 20_dws_traffic.sql 30_dws_trade.sql; do
  run_flink_sql "$file"
  log "  已提交 $file"
done

log "2/3 提交 DataStream 作业（Watermark + allowedLateness + 侧输出）"
"$FLINK_HOME/bin/flink" run -d -p 1 -c r1.LateDataJob "$JAR" --bootstrap-servers "$VM_HOST:9092" \
  > "$WORK_DIR/late-data-job.log" 2>&1 || die "提交失败，看日志 $WORK_DIR/late-data-job.log"

log "3/3 启动 Doris Routine Load（已经存在的作业会报错并跳过）"
mysql --force -h "$VM_HOST" -P 9030 -uroot < "$(render doris_routine_load.sql)" 2>&1 | grep -v 'already' || true

log "当前作业"
running_r1_jobs | while read -r job; do echo "  $job"; done
log "完成。Flink 网页：http://$VM_IP:8081。下一步运行 bin/replay.sh 开始回放"
