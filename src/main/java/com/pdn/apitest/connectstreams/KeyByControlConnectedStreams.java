package com.pdn.apitest.connectstreams;

import com.pdn.apitest.utils.ExecutionEnvironmentUtils;
import com.pdn.apitest.utils.KafkaUtils;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.RichCoFlatMapFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

/**
 * @author pdn
 *
 * 规则流controlStreamConsumer里面输入要被过滤的单词。
 * wordStreamConsumer里面输入所有的单词
 * 如果遇见了controlStreamConsumer的单词，则以后对应的单词不再进行输出
 */
public class KeyByControlConnectedStreams {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = ExecutionEnvironmentUtils.getExecutionEnvironment();

        FlinkKafkaConsumer011<String> controlStreamConsumer = KafkaUtils.getFlinkKafkaConsumer011("controlStream", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "a");
        DataStream<String> controlStream = env.addSource(controlStreamConsumer).setParallelism(3);
        KeyedStream<String, String> controlKeyStream = controlStream.keyBy(element -> element);

        FlinkKafkaConsumer011<String> wordStreamConsumer = KafkaUtils.getFlinkKafkaConsumer011("SideOutput", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "b");
        DataStream<String> wordStream = env.addSource(wordStreamConsumer).setParallelism(4);
        KeyedStream<String, String> wordKeyStream = wordStream.keyBy(element -> element);

//        被关联的2个流必须都被keyBy呢。这样才能保证数据被connect之后进行处理的时候。2个流相同的key的会被同一个算子实例处理。从而打到过滤的目前
        ConnectedStreams<String, String> connect = controlKeyStream.connect(wordKeyStream);

//        下面使用2种不同的状态，去实现该控制流的功能，所以此时则说明了，其是对于不同的key创建一个其对应的flatMap函数，所以该函数里面的每个key都会有自己单独对应的状态
//        SingleOutputStreamOperator<String> outputStreamOperator = connect.flatMap(new MyCoFlatMapFunctionValueState()).name("outputStreamOperator");
        SingleOutputStreamOperator<String> outputStreamOperator = connect.flatMap(new MyCoFlatMapFunctionMapState()).name("outputStreamOperator");

        outputStreamOperator.print("out");

        env.execute("ControlConnectedStreams");


    }

    public static class MyCoFlatMapFunctionValueState extends RichCoFlatMapFunction<String,String,String> {
        private ValueState<Boolean> blocked;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blocked",Boolean.class));

        }


        @Override
        public void flatMap1(String value, Collector<String> out) throws Exception {
            blocked.update(Boolean.TRUE);

        }

        @Override
        public void flatMap2(String dataValue, Collector<String> out) throws Exception {
            if (blocked.value() == null) {
                out.collect(dataValue);
            }
        }
    }

    public static class MyCoFlatMapFunctionMapState extends RichCoFlatMapFunction<String,String,String> {
        private MapState<String,Boolean> blocked;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            MapStateDescriptor<String, Boolean> mapStateDescriptor = new MapStateDescriptor<>("blocked", String.class, Boolean.class);
            blocked = getRuntimeContext().getMapState(mapStateDescriptor);

        }


        @Override
        public void flatMap1(String value, Collector<String> out) throws Exception {
            blocked.put(value,Boolean.TRUE);

        }

        @Override
        public void flatMap2(String dataValue, Collector<String> out) throws Exception {
            if (blocked.get(dataValue) == null) {
                out.collect(dataValue);
            }
        }
    }


}
