package com.pdn.apitest.window.funcation;

import com.pdn.apitest.beans.SensorReading;
import com.pdn.apitest.utils.ExecutionEnvironmentUtils;
import com.pdn.apitest.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.AggregateFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;

import java.time.Duration;

public class AggregateFunctionDemo {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = ExecutionEnvironmentUtils.getExecutionEnvironment();

        FlinkKafkaConsumer011<String> stringFlinkKafkaConsumer011 = KafkaUtils.getFlinkKafkaConsumer011("AggregateFunctionDemo", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "a");
        DataStream<String> inputStream = env.addSource(stringFlinkKafkaConsumer011);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print("in");

        SingleOutputStreamOperator<SensorReading> reduce = dataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//                        如果数据源中的某一个分区分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。
//                        我们称这类数据源为空闲输入或空闲源。在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。
//                        由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化。
//                        为了解决这个问题，你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。WatermarkStrategy 为此提供了一个工具接口：
                                .withIdleness(Duration.ofSeconds(10))
                )
                .keyBy("id")
                .timeWindow(Time.seconds(5))
                .reduce((v1, v2) -> (new SensorReading(v1.getId(), v1.getTimestamp(), v1.getTemperature() + v2.getTemperature())));
//                .aggregate(new MyAverageAggregate());

        reduce.print("out");

        env.execute("AggregateFunctionDemo");

    }
}

class MyAggregateFunction implements AggregateFunction<SensorReading, SensorReading, SensorReading> {


    //  创建累加器的初始值
    @Override
    public SensorReading createAccumulator() {
        return new SensorReading("chushi", -999L, 0.0);
    }

    //  使用当前处理的event，更新累加器的值
    @Override
    public SensorReading add(SensorReading value, SensorReading accumulator) {
        return new SensorReading(value.getId(), value.getTimestamp(), value.getTemperature() + accumulator.getTemperature());
    }

    //     从累加器里面获得结果
    @Override
    public SensorReading getResult(SensorReading accumulator) {
        return accumulator;
    }

    //merge类似于对不同的taskManager里面的累加器进行合并
    @Override
    public SensorReading merge(SensorReading a, SensorReading b) {
        Long timestamp;
        if (a.getTimestamp() < b.getTimestamp()) {
            timestamp = a.getTimestamp();
        } else {
            timestamp = b.getTimestamp();
        }
        return new SensorReading(a.getId(), timestamp, a.getTemperature() + b.getTemperature());
    }
}


//可以做到输入和输入的元素类型不一致
class MyAverageAggregate
        implements AggregateFunction<SensorReading, Tuple2<Double, Long>, Double> {
    @Override
    public Tuple2<Double, Long> createAccumulator() {
        return new Tuple2<>(0.0, 0L);
    }

    @Override
    public Tuple2<Double, Long> add(SensorReading value, Tuple2<Double, Long> accumulator) {
        return new Tuple2<> (value.getTemperature()+accumulator.f1,accumulator.f1+1L);
    }



    @Override
    public Double getResult(Tuple2<Double, Long> accumulator) {
        return ((double) accumulator.f0) / accumulator.f1;
    }

    @Override
    public Tuple2<Double, Long> merge(Tuple2<Double, Long> a, Tuple2<Double, Long> b) {
        return new Tuple2<>(a.f0 + b.f0, a.f1 + b.f1);
    }
}
