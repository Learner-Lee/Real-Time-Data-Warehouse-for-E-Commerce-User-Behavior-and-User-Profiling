package r1;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;

/**
 * ODS 用户行为，字段和回放器写进 Kafka 的 JSON 一致。
 * POJO（public 字段 + public 无参构造），Flink 才能用高效的 POJO 序列化器。
 */
public class UserBehavior {

    static final ObjectMapper MAPPER = new ObjectMapper();

    public long userId;
    public long itemId;
    public long categoryId;
    public String behavior;
    /** 事件时间（毫秒） */
    public long ts;
    /** 回放器注入的迟到秒数，正常数据为 null，只用于对账 */
    public Integer lateDelay;

    public UserBehavior() { }

    public UserBehavior(long userId, long itemId, long categoryId, String behavior, long ts) {
        this.userId = userId;
        this.itemId = itemId;
        this.categoryId = categoryId;
        this.behavior = behavior;
        this.ts = ts;
    }

    /** 解析一行 JSON；缺关键字段或格式不对时返回 null，由调用方丢弃 */
    public static UserBehavior fromJson(String json) {
        try {
            JsonNode node = MAPPER.readTree(json);
            if (!node.hasNonNull("user_id") || !node.hasNonNull("behavior") || !node.hasNonNull("ts")) {
                return null;
            }
            UserBehavior event = new UserBehavior(
                    node.get("user_id").asLong(),
                    node.path("item_id").asLong(),
                    node.path("category_id").asLong(),
                    node.get("behavior").asText(),
                    node.get("ts").asLong());
            if (node.hasNonNull("_late")) {
                event.lateDelay = node.get("_late").asInt();
            }
            return event;
        } catch (Exception e) {
            return null;
        }
    }

    /** 写回和 ODS 相同格式的 JSON（侧输出用） */
    public String toJson() {
        ObjectNode node = MAPPER.createObjectNode();
        node.put("user_id", userId);
        node.put("item_id", itemId);
        node.put("category_id", categoryId);
        node.put("behavior", behavior);
        node.put("ts", ts);
        if (lateDelay != null) {
            node.put("_late", lateDelay);
        }
        return node.toString();
    }
}
