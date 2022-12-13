package com.pdn.apitest.window.funcation;

import com.pdn.apitest.beans.SensorReading;
import com.pdn.apitest.utils.ExecutionEnvironmentUtils;
import com.pdn.apitest.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import scala.Tuple4;

import java.time.Duration;

public class ReduceProcessFuncation {
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

        dataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//                        如果数据源中的某一个分区分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。
//                        我们称这类数据源为空闲输入或空闲源。在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。
//                        由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化。
//                        为了解决这个问题，你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。WatermarkStrategy 为此提供了一个工具接口：
                                .withIdleness(Duration.ofSeconds(10))
                )
                .keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(5))
                .reduce((r1, r2) -> r1.getTemperature() > r2.getTemperature() ? r2 : r1,
                        new ProcessWindowFunction<SensorReading, Tuple3<SensorReading, String, Long>, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<SensorReading, Tuple3<SensorReading, String, Long>, String, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<Tuple3<SensorReading, String, Long>> out) throws Exception {
                                out.collect(new Tuple3<>(elements.iterator().next(), context.window().getStart() + "$$$" + context.window().getEnd(), context.currentWatermark()));
                            }
                        }).print("ReduceProcessFuncation");

        env.execute("ReduceProcessFuncation");

    }
}
