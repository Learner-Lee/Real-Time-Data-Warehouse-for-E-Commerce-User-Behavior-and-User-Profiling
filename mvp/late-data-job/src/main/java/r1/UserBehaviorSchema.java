package r1;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.StandardCharsets;

/**
 * 在 Kafka Source 里直接把字节解析成 UserBehavior。
 * 这样 Watermark 可以在 Source 上按分区生成（见 LateDataJob），解析失败的消息返回 null，Flink 会跳过它。
 */
public class UserBehaviorSchema implements DeserializationSchema<UserBehavior> {

    @Override
    public UserBehavior deserialize(byte[] message) {
        return message == null ? null : UserBehavior.fromJson(new String(message, StandardCharsets.UTF_8));
    }

    @Override
    public boolean isEndOfStream(UserBehavior nextElement) {
        return false;
    }

    @Override
    public TypeInformation<UserBehavior> getProducedType() {
        return TypeInformation.of(UserBehavior.class);
    }
}
