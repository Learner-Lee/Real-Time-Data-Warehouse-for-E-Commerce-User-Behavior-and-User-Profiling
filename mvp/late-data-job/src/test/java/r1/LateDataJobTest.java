package r1;

import org.apache.flink.api.common.eventtime.Watermark;
import org.apache.flink.api.common.eventtime.WatermarkGenerator;
import org.apache.flink.api.common.eventtime.WatermarkOutput;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.CloseableIterator;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class LateDataJobTest {

    @Test
    void threeLinesOfDefenseForLateData() throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // 按到达顺序排列；窗口 1 分钟，Watermark 容忍 5 秒，允许迟到 1 分钟
        List<UserBehavior> arrivals = List.of(
                pv(10_000),    // 进窗口 [0s, 60s)
                pv(30_000),    // 进窗口 [0s, 60s)
                pv(70_000),    // Watermark 推到 64.999s → [0s, 60s) 第一次触发，结果 2
                pv(50_000),    // 迟到，但还在允许迟到时间内 → [0s, 60s) 再触发一次，结果 3
                pv(130_000),   // Watermark 推到 124.999s → [60s, 120s) 触发，同时清理 [0s, 60s)
                pv(20_000));   // [0s, 60s) 已清理，超过允许迟到时间 → 侧输出

        DataStream<UserBehavior> events = env.fromData(arrivals).assignTimestampsAndWatermarks(
                WatermarkStrategy.<UserBehavior>forGenerator(ctx -> new PerEventWatermarks())
                        .withTimestampAssigner((event, recordTimestamp) -> event.ts));
        SingleOutputStreamOperator<BehaviorCount> counts = LateDataJob.countPerMinute(events);
        DataStream<String> merged = counts.map(c -> "count:" + c.windowStart + "=" + c.count)
                .union(counts.getSideOutput(LateDataJob.LATE).map(e -> "late:" + e.ts));

        List<String> out = new ArrayList<>();
        try (CloseableIterator<String> it = merged.executeAndCollect()) {
            it.forEachRemaining(out::add);
        }

        // 两路输出合并后彼此的先后不固定，所以分开比较；主输出内部的顺序是确定的
        assertEquals(List.of("count:0=2", "count:0=3", "count:60000=1", "count:120000=1"),
                out.stream().filter(s -> s.startsWith("count:")).toList());
        assertEquals(List.of("late:20000"), out.stream().filter(s -> s.startsWith("late:")).toList());
    }

    @Test
    void jsonRoundTripKeepsInjectedLateMarker() {
        UserBehavior event = UserBehavior.fromJson(
                "{\"user_id\":1,\"item_id\":2,\"category_id\":3,\"behavior\":\"pv\",\"ts\":1511539200123,\"_late\":180}");
        assertEquals(180, event.lateDelay);
        assertEquals(event.toJson(), UserBehavior.fromJson(event.toJson()).toJson());
        assertNull(UserBehavior.fromJson("不是 JSON"));
        assertNull(UserBehavior.fromJson("{\"user_id\":1}"));   // 缺 behavior 和 ts
    }

    @Test
    void countJsonUsesBeijingTimeAndDorisColumnNames() {
        BehaviorCount count = new BehaviorCount(1511539260000L, "pv", 42, 1511539260000L);
        assertEquals("{\"window_start\":\"2017-11-25 00:01:00.000\",\"behavior\":\"pv\",\"cnt\":42,"
                + "\"emit_time\":\"2017-11-25 00:01:00.000\"}", count.toJson());
    }

    private static UserBehavior pv(long ts) {
        return new UserBehavior(1, 100, 10, "pv", ts);
    }

    /** 每来一条事件就推进 Watermark（标点式），让测试结果确定；生产上用周期性 Watermark */
    static class PerEventWatermarks implements WatermarkGenerator<UserBehavior> {
        private long maxTs = Long.MIN_VALUE;

        @Override
        public void onEvent(UserBehavior event, long eventTimestamp, WatermarkOutput output) {
            maxTs = Math.max(maxTs, eventTimestamp);
            output.emitWatermark(new Watermark(maxTs - LateDataJob.OUT_OF_ORDERNESS.toMillis() - 1));
        }

        @Override
        public void onPeriodicEmit(WatermarkOutput output) {
            // 标点式 Watermark 不需要周期性发送
        }
    }
}
