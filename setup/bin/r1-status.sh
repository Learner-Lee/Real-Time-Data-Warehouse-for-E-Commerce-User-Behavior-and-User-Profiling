#!/usr/bin/env bash
# 查看 R1 各组件是否在运行、端口通不通、各占多少内存
source "$(dirname "$0")/../lib/common.sh"

port_open() { (exec 3<>"/dev/tcp/$VM_HOST/$1") 2>/dev/null; }

show() {   # 用法：show 名称 进程匹配 端口（没有固定端口就写 -）
  local name=$1 pattern=$2 port=$3 pids state mem port_state="-"
  pids=$(pgrep -f "$pattern" | paste -sd, - || true)
  if [[ -z "$pids" ]]; then
    state="停止"
    mem="-"
  else
    state="运行中"
    mem=$(ps -o rss= -p "$pids" | awk '{ s += $1 } END { printf "%.1fG", s / 1024 / 1024 }')
  fi
  if [[ "$port" != "-" ]]; then
    if port_open "$port"; then port_state="$port 通"; else port_state="$port 不通"; fi
  fi
  printf '%-10s %-6s %-12s %s\n' "$name" "$state" "$port_state" "$mem"
}

printf '%-10s %-6s %-12s %s\n' "组件" "状态" "端口" "内存"
show "MySQL"    'mysqld'                              3306
show "Kafka"    'kafka\.Kafka'                        9092
show "HBase"    'hbase\.master\.HMaster'              16010
show "DorisFE"  'DorisFE'                             9030
show "DorisBE"  'doris_be'                            9050
show "FlinkJM"  'StandaloneSessionClusterEntrypoint'  8081
show "FlinkTM"  'TaskManagerRunner'                   -
echo
free -h | awk 'NR == 2 { print "虚拟机内存：已用 " $3 " / 共 " $2 "，可用 " $7 }'
df -h "$DATA_DIR" | awk 'NR == 2 { print "数据盘：已用 " $3 " / 共 " $2 "，剩余 " $4 }'
