package com.pdn.apitest.utils;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

public class KafkaUtils {
    public static KafkaSource<String> getKafkaSourcer(String groupId, String bootstrapServers, String topics) {
        //        flink在实现一次仅有一次的时候，是将偏移量存储在checkPoint里面的，所以其不依赖kafka自己存储的偏移量。
//        其不需要自动的提交。但是其也可以进行自动的提交。
//        提交到kafka里面的偏移量。则仅仅用于监控。
        return KafkaSource.<String>builder().setGroupId(groupId).setBootstrapServers(bootstrapServers).setTopics(topics)
                .setStartingOffsets(OffsetsInitializer.latest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();
    }


    public static FlinkKafkaConsumer011<String> getFlinkKafkaConsumer011(String groupId, String bootstrapServers, String topics) {
        //        flink在实现一次仅有一次的时候，是将偏移量存储在checkPoint里面的，所以其不依赖kafka自己存储的偏移量。
//        其不需要自动的提交。但是其也可以进行自动的提交。
//        提交到kafka里面的偏移量。则仅仅用于监控。
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", bootstrapServers);
        properties.setProperty("group.id", groupId);
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");
        return new FlinkKafkaConsumer011<>(topics, new SimpleStringSchema(), properties);
    }

}
