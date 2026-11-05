-- Doris Routine Load：Doris 自己持续消费 Kafka，把数据导进表里（另一种导入方式，和 Flink 写入对照着看）
-- 执行：mysql --force -h bigdata01 -P 9030 -uroot < doris_routine_load.sql（作业已存在时会报错，--force 让它继续往下执行）
-- @VM_HOST@、@LABEL_SUFFIX@ 由 bin/start_jobs.sh 替换
-- group.id 每次运行带不同后缀：Doris BE 会缓存 Kafka 消费者，属性完全相同就复用。一个消费者一旦卡住（比如读的 offset 被删），
-- 之后每一轮都会被拿出来复用、一直 0 行（2026-11-04 实测：同一个坏消费者被复用了一整天）。换后缀等于每轮都用新消费者
USE r1_dw;

-- ODS 原始行为 → ods_user_behavior。回放器不用事务写 Kafka，所以不需要 read_committed
CREATE ROUTINE LOAD r1_dw.rl_ods_user_behavior ON ods_user_behavior
COLUMNS (ts, user_id, item_id, category_id, behavior, late_delay)
PROPERTIES (
  "format" = "json",
  "jsonpaths" = "[\"$.ts\", \"$.user_id\", \"$.item_id\", \"$.category_id\", \"$.behavior\", \"$._late\"]",
  "desired_concurrent_number" = "1"
)
FROM KAFKA (
  "kafka_broker_list" = "@VM_HOST@:9092",
  "kafka_topic" = "ods_user_behavior",
  "property.group.id" = "doris-r1-ods-@LABEL_SUFFIX@",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- DataStream 作业的窗口结果 → dws_behavior_1m_stream。上游用事务写 Kafka，这里只读已提交的数据
CREATE ROUTINE LOAD r1_dw.rl_behavior_1m_stream ON dws_behavior_1m_stream
COLUMNS (window_start, behavior, cnt, emit_time)
PROPERTIES (
  "format" = "json",
  "desired_concurrent_number" = "1"
)
FROM KAFKA (
  "kafka_broker_list" = "@VM_HOST@:9092",
  "kafka_topic" = "dws_behavior_1m_stream",
  "property.group.id" = "doris-r1-behavior-stream-@LABEL_SUFFIX@",
  "property.isolation.level" = "read_committed",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);

-- DataStream 作业的侧输出（超过允许迟到时间的数据）→ dwd_late_events
CREATE ROUTINE LOAD r1_dw.rl_late_events ON dwd_late_events
COLUMNS (ts, user_id, item_id, category_id, behavior, late_delay)
PROPERTIES (
  "format" = "json",
  "jsonpaths" = "[\"$.ts\", \"$.user_id\", \"$.item_id\", \"$.category_id\", \"$.behavior\", \"$._late\"]",
  "desired_concurrent_number" = "1"
)
FROM KAFKA (
  "kafka_broker_list" = "@VM_HOST@:9092",
  "kafka_topic" = "ods_late_events",
  "property.group.id" = "doris-r1-late-events-@LABEL_SUFFIX@",
  "property.isolation.level" = "read_committed",
  "property.kafka_default_offsets" = "OFFSET_BEGINNING"
);
