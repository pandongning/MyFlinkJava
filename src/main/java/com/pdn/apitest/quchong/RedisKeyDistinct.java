package com.pdn.apitest.quchong;

import com.pdn.apitest.utils.RedisUtils;
import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import redis.clients.jedis.Jedis;

import java.time.Duration;
import java.util.Properties;

public class RedisKeyDistinct {
    public static void main(String[] args) throws Exception {
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
        SingleOutputStreamOperator<Tuple2<String, String>> map = dataStream.map(x -> new Tuple2<String, String>(x.split(",")[0], x.split(",")[1]))
                .returns(Types.TUPLE(Types.STRING, Types.STRING));

        SingleOutputStreamOperator<Tuple2<String, String>> flatMap = map.flatMap(new RichFlatMapFunction<Tuple2<String, String>, Tuple2<String, String>>() {

            Jedis jedis = null;

            Cache<Object, Object> cacheBuilder = null;

            @Override
            public void open(Configuration parameters) throws Exception {
                jedis = new RedisUtils("127.0.0.1", 6379).getJedisConnect();
                cacheBuilder = CacheBuilder.newBuilder().
                        maximumSize(2).expireAfterAccess(Duration.ofSeconds(5)).build();
            }

            @Override
            public void flatMap(Tuple2<String, String> in, Collector<Tuple2<String, String>> out) throws Exception {
                String value = null;
                value = (String) cacheBuilder.getIfPresent(in.f0);
                System.out.println("cacheBuilder__" + value);
                if (value == null) {
                    value = jedis.get(in.f0);
                    System.out.println("jedis___" + value);
                    if (value != null) {
                        cacheBuilder.put(in.f0, value);
                    } else {
                        cacheBuilder.put(in.f0, in.f1);
                        jedis.set(in.f0, in.f1);
                        out.collect(new Tuple2<String, String>(in.f0, in.f1));
                    }
                }


            }
        });

        flatMap.print("RedisKeyDistinct");

        env.execute("RedisKeyDistinctExample");
    }
}
