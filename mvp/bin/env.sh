#!/usr/bin/env bash
# R1 MVP 各脚本共用：复用环境安装脚本里的配置和函数（默认在 ~/r1-setup，可以用 R1_SETUP=路径 指定）

R1_SETUP=${R1_SETUP:-$HOME/r1-setup}
if [[ ! -f "$R1_SETUP/lib/common.sh" ]]; then
  echo "找不到 $R1_SETUP/lib/common.sh：先按 setup 的 README 装好环境，或者用 R1_SETUP=路径 指定" >&2
  exit 1
fi
# shellcheck source=/dev/null
source "$R1_SETUP/lib/common.sh"

MVP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
R1_DATA="$DATA_DIR/r1"              # 回放文件和维表 CSV（路径和 sql/00_load_dims.sql 一致）
WORK_DIR="$DATA_DIR/r1-work"        # 渲染后的 SQL（里面有密码，不进 git）和各种日志
LABEL_SUFFIX=$(date +%Y%m%d%H%M%S)  # Doris 导入 label、Kafka 事务 ID 前缀每次提交都要不同
mkdir -p "$R1_DATA" "$WORK_DIR"

app_mysql() { MYSQL_PWD="$MYSQL_APP_PASSWORD" mysql -h "$VM_HOST" -u"$MYSQL_APP_USER" r1_shop "$@"; }

# 把 sql/ 下模板里的 @...@ 换成实际值，生成到 $WORK_DIR，返回生成文件的路径
render() {
  sed -e "s|@VM_HOST@|$VM_HOST|g" \
      -e "s|@MYSQL_APP_USER@|$MYSQL_APP_USER|g" \
      -e "s|@MYSQL_APP_PASSWORD@|$MYSQL_APP_PASSWORD|g" \
      -e "s|@LABEL_SUFFIX@|$LABEL_SUFFIX|g" \
      -e "s|@DATA_DIR@|$DATA_DIR|g" \
      "$MVP_DIR/sql/$1" > "$WORK_DIR/$1"
  echo "$WORK_DIR/$1"
}

# 用 Flink SQL Client 提交一个 SQL 文件
run_flink_sql() {
  local file=$1 rendered
  rendered=$(render "$file")
  "$FLINK_HOME/bin/sql-client.sh" -f "$rendered" > "$WORK_DIR/${file%.sql}.log" 2>&1 ||
    die "提交 $file 失败，看日志 $WORK_DIR/${file%.sql}.log"
}

# 消息时间戳统一用 Kafka 写入时的时间（LogAppendTime）。Flink DataStream 的 Kafka sink 默认把事件时间（2017 年）
# 写成消息时间戳，而日志滚动和 48 小时保留都按时间戳算：几分钟内整段日志就被当成过期删除，下游从 offset 0 读会越界、丢数据。
# 事件时间在消息内容的 ts 字段里，Watermark 也从 ts 取，不依赖 Kafka 时间戳
create_topic() {   # 用法：create_topic 名称 分区数
  "$KAFKA_HOME/bin/kafka-topics.sh" --bootstrap-server "$VM_HOST:9092" --create --if-not-exists \
    --topic "$1" --partitions "$2" --replication-factor 1 --config message.timestamp.type=LogAppendTime
  # topic 删除后可能被客户端自动重建（这时上面的 --if-not-exists 会跳过），所以再显式设置一次
  "$KAFKA_HOME/bin/kafka-configs.sh" --bootstrap-server "$VM_HOST:9092" --alter --entity-type topics \
    --entity-name "$1" --add-config message.timestamp.type=LogAppendTime >/dev/null
}

# 正在运行的 R1 作业（名字以 r1- 开头），每行一个作业 ID
running_r1_jobs() {
  "$FLINK_HOME/bin/flink" list -r 2>/dev/null | awk -F' : ' '$3 ~ /^r1-/ { print $2 }' || true
}
