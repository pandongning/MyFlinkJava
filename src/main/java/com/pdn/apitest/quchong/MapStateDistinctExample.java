package com.pdn.apitest.quchong;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.util.Properties;

public class MapStateDistinctExample {
    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env =
                StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);

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
                .returns(Types.TUPLE(Types.STRING,Types.STRING));

//        下面必须进行keyBy。因为去重是根据key进行去重的，
        KeyedStream<Tuple2<String, String>, String> keyedStream = map.keyBy(x -> x.f0);

        SingleOutputStreamOperator<String> quchong = keyedStream.flatMap(new RichFlatMapFunction<Tuple2<String, String>, String>() {
            org.apache.flink.api.common.state.MapState<String, Integer> mapState = null;

            //           每个算字都会执行一次，比如并行度为3，则此时会执行3次
            @Override
            public void open(Configuration parameters) throws Exception {
                System.out.println("我又执行一次呢");
                super.open(parameters);
                StateTtlConfig build = StateTtlConfig.newBuilder(Time.seconds(3))
                        .setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
                        .setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
                        .build();
                MapStateDescriptor<String, Integer> mapStateDescriptor = new MapStateDescriptor<>("quchong", String.class, Integer.class);
                mapStateDescriptor.enableTimeToLive(build);
                mapState = getRuntimeContext().getMapState(mapStateDescriptor);

            }

            @Override
            public void flatMap(Tuple2<String, String> value, Collector<String> out) throws Exception {
                if (!mapState.contains(value.f0)) {
                    out.collect(value.f0 + "$$" + value.f1.toString());
                    mapState.put(value.f0, 1);
                }

            }
        }).returns(Types.STRING);


        quchong.print("quchong");


        env.execute("KafkaConnector");

    }
}
