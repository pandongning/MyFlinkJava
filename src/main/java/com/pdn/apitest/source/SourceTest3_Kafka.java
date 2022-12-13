package com.pdn.apitest.source;/**
 * Copyright (c) 2018-2028 尚硅谷 All Rights Reserved
 * <p>
 * Project: FlinkTutorial
 * Package: com.atguigu.apitest.source
 * Version: 1.0
 * <p>
 * Created by wushengran on 2020/11/7 11:54
 */

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.util.Properties;

/**
 * @ClassName: SourceTest3_Kafka
 * @Description:
 * @Version: 1.0
 */
public class SourceTest3_Kafka {
    public static void main(String[] args) throws Exception {
//        BasicConfigurator.configure();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093");
        properties.setProperty("group.id", "consumer-group");
        properties.setProperty("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        properties.setProperty("auto.offset.reset", "latest");

        //泛型String表示kafka里面每条记录的类型
//        为了实现有且仅有一次，则不能配置下面的代码
//        properties.setProperty("auto.offset.reset", "latest")
//        和properties.setProperty("enable.auto.commit",)
//
//        flink在实现一次仅有一次的时候，是将偏移量存储在checkPoint里面的，所以其不依赖kafka自己存储的偏移量。
//        其不需要自动的提交。但是其也可以进行自动的提交。
//
//        提交到kafka里面的偏移量。则仅仅用于监控。
        DataStream<String> dataStream = env.addSource(new FlinkKafkaConsumer011<String>("a", new SimpleStringSchema(), properties));

        // 打印输出
        dataStream.print();

        env.execute();
    }
}
