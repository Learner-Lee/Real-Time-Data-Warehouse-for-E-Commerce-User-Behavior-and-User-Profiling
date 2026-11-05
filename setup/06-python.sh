#!/usr/bin/env bash
# 第 6 步：Python 虚拟环境（回放器、画像模型要用）
source "$(dirname "$0")/lib/common.sh"
check_env

if [[ ! -d "$PY_VENV" ]]; then
  log "创建虚拟环境 $PY_VENV"
  python3 -m venv "$PY_VENV"
fi

log "安装依赖（用阿里云 PyPI 镜像）"
"$PY_VENV/bin/pip" install -i "$PIP_INDEX" --upgrade pip
"$PY_VENV/bin/pip" install -i "$PIP_INDEX" \
  confluent-kafka pymysql duckdb numpy pandas pyarrow scikit-learn xgboost

log "验证"
"$PY_VENV/bin/python" -c "import confluent_kafka, pymysql, duckdb, pandas, sklearn, xgboost; print('依赖全部可用：confluent-kafka', confluent_kafka.version()[0], '/ duckdb', duckdb.__version__, '/ pandas', pandas.__version__, '/ xgboost', xgboost.__version__)"
log "完成。以后用 source $PY_VENV/bin/activate 进入这个环境。下一步运行 smoke/run-smoke.sh 做冒烟测试"
