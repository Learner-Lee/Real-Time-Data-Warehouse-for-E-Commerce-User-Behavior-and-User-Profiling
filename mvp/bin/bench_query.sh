#!/usr/bin/env bash
# 测 Doris 查询延迟：同一条 SQL 跑 N 次，输出 P50 / P95 / P99（毫秒，含 mysql 客户端每次启动的十几毫秒开销）。
# 用法：bin/bench_query.sh [次数] [SQL]。不带 SQL 时测大屏的「每天 UV」查询（BITMAP 合并）
source "$(dirname "$0")/env.sh"

n=${1:-100}
sql=${2:-"SELECT DATE(window_start), BITMAP_UNION_COUNT(user_id) FROM r1_dw.dws_uv_bitmap_1m GROUP BY DATE(window_start)"}

log "执行 $n 次：$sql"
durations=()
for ((i = 0; i < n; i++)); do
  start=$(date +%s%N)
  doris_sql "$sql" >/dev/null
  end=$(date +%s%N)
  durations+=($(((end - start) / 1000000)))
done
printf '%s\n' "${durations[@]}" | sort -n | awk -v n="$n" '
  { v[NR] = $1 }
  function at(p,   i) { i = int(n * p); return v[i < 1 ? 1 : i] }
  END { printf "P50 %d 毫秒｜P95 %d 毫秒｜P99 %d 毫秒｜最大 %d 毫秒\n", at(0.5), at(0.95), at(0.99), v[n] }'
