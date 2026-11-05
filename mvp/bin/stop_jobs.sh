#!/usr/bin/env bash
# 取消 R1 的 Flink 作业。加 --with-doris 同时停止 Doris Routine Load（停止后不能恢复，下次 start_jobs.sh 会重新创建）
source "$(dirname "$0")/env.sh"

for job in $(running_r1_jobs); do
  if "$FLINK_HOME/bin/flink" cancel "$job" >/dev/null 2>&1; then
    log "已取消 Flink 作业 $job"
  else
    warn "取消 $job 失败"
  fi
done

if [[ "${1:-}" == "--with-doris" ]]; then
  for routine_load in rl_ods_user_behavior rl_behavior_1m_stream rl_late_events; do
    if doris_sql "STOP ROUTINE LOAD FOR r1_dw.$routine_load" 2>/dev/null; then
      log "已停止 Routine Load $routine_load"
    fi
  done
fi
log "完成"
