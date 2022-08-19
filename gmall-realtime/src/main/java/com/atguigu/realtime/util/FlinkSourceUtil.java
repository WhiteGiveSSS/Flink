package com.atguigu.realtime.util;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

/**
 * @Author lizhenchao@atguigu.cn
 * @Date 2021/6/25 9:20
 */
public class FlinkSourceUtil {
    // 得到一个kafka source
    public static FlinkKafkaConsumer<String> getKafkaSource(String groupId, String topic) {
        Properties props = new Properties();
        props.setProperty("bootstrap.servers", "hadoop162:9092,hadoop163:9092,hadoop164:9092");
        props.setProperty("group.id", groupId);
        props.setProperty("auto.offset.reset", "latest");
        props.setProperty("isolation.level", "read_committed");
        return new FlinkKafkaConsumer<String>(
                topic,
                new SimpleStringSchema(),
                props
        );
    }
}
