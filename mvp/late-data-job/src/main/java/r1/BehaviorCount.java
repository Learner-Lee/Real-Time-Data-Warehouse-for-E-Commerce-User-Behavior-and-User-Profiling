package r1;

import com.fasterxml.jackson.databind.node.ObjectNode;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

/** 一个窗口一种行为的计数结果。迟到数据让窗口再次触发时，会输出同一窗口的新结果，Doris 按主键覆盖 */
public class BehaviorCount {

    private static final DateTimeFormatter FORMAT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS").withZone(ZoneId.of("Asia/Shanghai"));

    /** 窗口开始时间（毫秒） */
    public long windowStart;
    public String behavior;
    public long count;
    /** 这次结果的输出时间（处理时间，毫秒） */
    public long emitTime;

    public BehaviorCount() { }

    public BehaviorCount(long windowStart, String behavior, long count, long emitTime) {
        this.windowStart = windowStart;
        this.behavior = behavior;
        this.count = count;
        this.emitTime = emitTime;
    }

    /** 字段名和 Doris 表 dws_behavior_1m_stream 的列名一致，Routine Load 直接按名字导入 */
    public String toJson() {
        ObjectNode node = UserBehavior.MAPPER.createObjectNode();
        node.put("window_start", FORMAT.format(Instant.ofEpochMilli(windowStart)));
        node.put("behavior", behavior);
        node.put("cnt", count);
        node.put("emit_time", FORMAT.format(Instant.ofEpochMilli(emitTime)));
        return node.toString();
    }
}
