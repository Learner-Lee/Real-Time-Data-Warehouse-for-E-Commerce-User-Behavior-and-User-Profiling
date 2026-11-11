#!/usr/bin/env bash
# 用户画像（离线批处理，回放结束后运行）：
#   1. 建画像相关的表，从 ODS 生成用户 × 天的 DWS（dws_user_behavior_1d）
#   2. 算 55 个用户特征（dws_user_feature）→ RFM + K-Means 分群 → XGBoost 未来 3 天购买预测
#   3. 标签写回 Doris（ads_user_profile_tag），报告和模型放在 $WORK_DIR/profile/
# 可以重复运行，结果覆盖写入
source "$(dirname "$0")/env.sh"
check_env
check_password

log "1/2 建表，从 ODS 生成用户 × 天的 DWS"
mysql -h "$VM_HOST" -P 9030 -uroot < "$MVP_DIR/sql/profile_tables.sql" || die "执行 sql/profile_tables.sql 失败"

log "2/2 特征 → 分群 → 复购模型 → 标签"
export MYSQL_APP_PASSWORD   # Python 从环境变量读密码，不放在命令行里
"$PY_VENV/bin/python" "$MVP_DIR/user_profile/run_profile.py" \
  --doris-host "$VM_HOST" \
  --mysql-host "$VM_HOST" \
  --mysql-user "$MYSQL_APP_USER" \
  --dim-user "$R1_DATA/dim_user.csv" \
  --out "$WORK_DIR/profile" \
  "$@"
