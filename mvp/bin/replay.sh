#!/usr/bin/env bash
# 开始回放。后面的参数原样传给 replayer.py，例如：
#   bin/replay.sh                              默认：1 秒回放 10 分钟事件时间，注入乱序和迟到
#   bin/replay.sh --speed 3600                 更快：1 秒回放 1 小时
#   bin/replay.sh --speed 0 --max-rate 20000   压测：不按事件时间控速，每秒最多 2 万条，看能不能稳住
source "$(dirname "$0")/env.sh"
check_env
check_password

[[ -s "$R1_DATA/behavior_sorted.csv" ]] || die "缺少回放文件：先运行 bin/prepare.sh"
export MYSQL_APP_PASSWORD   # 回放器从环境变量读密码，不放在命令行里
"$PY_VENV/bin/python" "$MVP_DIR/replayer/replayer.py" \
  --input "$R1_DATA/behavior_sorted.csv" \
  --dim-item "$R1_DATA/dim_item.csv" \
  --bootstrap-servers "$VM_HOST:9092" \
  --mysql-host "$VM_HOST" \
  --mysql-user "$MYSQL_APP_USER" \
  "$@" 2>&1 | tee "$WORK_DIR/replay-$(date +%H%M%S).log"
