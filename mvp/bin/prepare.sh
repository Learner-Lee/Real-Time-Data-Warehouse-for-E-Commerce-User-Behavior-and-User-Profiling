#!/usr/bin/env bash
# 准备回放数据和维表，输出到 /data/r1。
# 用法：
#   bin/prepare.sh                                   不带参数：生成 50 万行模拟数据（没下载真实数据时先用它跑通）
#   bin/prepare.sh /data/UserBehavior.csv            真实数据：默认按用户抽 1%（约 100 万行）
#   bin/prepare.sh /data/UserBehavior.csv 10         真实数据：抽 10%
source "$(dirname "$0")/env.sh"

if [[ $# -ge 1 ]]; then
  src=$1
  sample_mod=${2:-100}
  [[ -f "$src" ]] || die "找不到数据文件 $src"
else
  src="$R1_DATA/UserBehavior_synthetic.csv"
  sample_mod=1
  log "没有指定数据文件：先生成模拟数据"
  "$PY_VENV/bin/python" "$MVP_DIR/data/gen_synthetic.py" --out "$src"
fi

log "抽样、排序、生成维表"
"$PY_VENV/bin/python" "$MVP_DIR/data/prepare_data.py" --input "$src" --out-dir "$R1_DATA" --sample-mod "$sample_mod"
log "完成。下一步运行 bin/setup.sh"
