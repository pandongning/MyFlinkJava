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

import java.util.Objects;

/**
 * @author pdn
 */
public class NoKeyByControlConnectedStreams {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = ExecutionEnvironmentUtils.getExecutionEnvironment();

        FlinkKafkaConsumer011<String> controlStreamConsumer = KafkaUtils.getFlinkKafkaConsumer011("controlStream", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "a");
        DataStream<String> controlStream = env.addSource(controlStreamConsumer).setParallelism(3);

        FlinkKafkaConsumer011<String> wordStreamConsumer = KafkaUtils.getFlinkKafkaConsumer011("SideOutput", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "b");
        DataStream<String> wordStream = env.addSource(wordStreamConsumer).setParallelism(4);

        ConnectedStreams<String, String> connect = controlStream.connect(wordStream);

//        下面的方法会报错Caused by: java.lang.NullPointerException: Keyed state can only be used on a 'keyed stream', i.e., after a 'keyBy()' operation.
//        说明对于ConnectedStreams使用状态的时候，必须是使用keyedStream。这个时候才能包装key相同的数据被发送到同一个slot上。此时才可以共享同一个状态
        /**
         * 值得一提的是，ConnectedStreams 也可以直接调用.keyBy()进行按键分区的操作，得到的还是一个ConnectedStreams：
         * connectedStreams.keyBy(keySelector1, keySelector2);
         * 这里传入两个参数 keySelector1 和 keySelector2，是两条流中各自的键选择器；当然也可以直接传入键的位置值（keyPosition），或者键的字段名（field），
         * 这与普通的 keyBy 用法完全一致。ConnectedStreams 进行 keyBy 操作，其实就是把两条流中 key 相同的数据放到了一起， 然后针对来源的流再做各自处理，
         * 这在一些场景下非常有用。另外，我们也可以在合并之前就将两条流分别进行 keyBy,得到的 KeyedStream 再进行连接（connect）操作，效果是一样的。
         * 要注意两条流定义的键的类型必须相同，否则会抛出异常。
         */
        SingleOutputStreamOperator<String> outputStreamOperator = connect.flatMap(new MyCoFlatMapFunctionValueState()).name("outputStreamOperator");
//        SingleOutputStreamOperator<String> outputStreamOperator = connect.flatMap(new MyCoFlatMapFunctionMapState()).name("outputStreamOperator");

        outputStreamOperator.print("out");

        env.execute("ControlConnectedStreams");


    }

    public static class MyCoFlatMapFunctionValueState extends RichCoFlatMapFunction<String, String, String> {
        private ValueState<Boolean> blocked;


        @Override
        public void open(Configuration parameters) throws Exception {
            super.open(parameters);
            blocked = getRuntimeContext().getState(new ValueStateDescriptor<Boolean>("blocked", Boolean.class));

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

    public static class MyCoFlatMapFunctionMapState extends RichCoFlatMapFunction<String, String, String> {


        @Override
        public void flatMap1(String value, Collector<String> out) throws Exception {
            out.collect(value);
        }

        @Override
        public void flatMap2(String dataValue, Collector<String> out) throws Exception {
            if (!Objects.equals(dataValue, "a")){
                out.collect(dataValue);
            }
        }
    }


}
