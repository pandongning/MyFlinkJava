package com.pdn.apitest.window;

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
import org.apache.flink.util.OutputTag;

import java.time.Duration;

public class SideOutput {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = ExecutionEnvironmentUtils.getExecutionEnvironment();

        FlinkKafkaConsumer011<String> stringFlinkKafkaConsumer011 = KafkaUtils.getFlinkKafkaConsumer011("SideOutput", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "a");
        DataStream<String> inputStream = env.addSource(stringFlinkKafkaConsumer011);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        dataStream.print("in");

        final OutputTag<SensorReading> lateOutputTag = new OutputTag<SensorReading>("late-data"){};

        SingleOutputStreamOperator<Tuple3<SensorReading, String, Long>> reduce = dataStream
                .assignTimestampsAndWatermarks(
                                 WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//                        如果数据源中的某一个分区分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。
//                        我们称这类数据源为空闲输入或空闲源。在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。
//                        由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化。
//                        为了解决这个问题，你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。WatermarkStrategy 为此提供了一个工具接口：
                                .withIdleness(Duration.ofSeconds(10))
                )
                .keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(5))
                .sideOutputLateData(lateOutputTag)
                .reduce((r1, r2) -> r1.getTemperature() > r2.getTemperature() ? r2 : r1,
                        new ProcessWindowFunction<SensorReading, Tuple3<SensorReading, String, Long>, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<SensorReading, Tuple3<SensorReading, String, Long>, String, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<Tuple3<SensorReading, String, Long>> out) throws Exception {
                                out.collect(new Tuple3<>(elements.iterator().next(), context.window().getStart() + "$$$" + context.window().getEnd(), context.currentWatermark()));
                            }
                        });

        /**
         * in:3> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.0}
         * in:3> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.1}
         * in:3> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.2}
         * in:3> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.0}
         * in:3> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.1}
         * in:3> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.2}
         * in:3> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.0}
         * in:3> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.1}
         * in:3> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.2}
         * in:3> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.0}
         * in:3> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.1}
         * in:3> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.2}
         * in:1> SensorReading{id='sensor_5', timestamp=1599990797000, temperature=5.0}
         * reduce:1> (SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.0},1599990790000$$$1599990795000,1599990794999)
         * reduce:2> (SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.0},1599990790000$$$1599990795000,1599990794999)
         * timestamp=1599990797000的时候，窗口的操作已经被触发，所以后面再输入timestamp=1599990791000的记录，则属于延迟的数据。
         * 输入延迟的数据。可以看见其「没有更新」以前窗口计算的结果。因为应超过了窗口的结束时间，窗口的元数据信息已经被删除呢
         * 所以此处输出为不再触发窗口的操作。如果要触发操作，需要使用allowedLateness的语法。才可以更新之前输出的结果
         * in:3> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=0.0}
         * late-data:2> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=0.0}
         *  因为有侧输出流，所以任何时候延迟的数据都会被输出到侧流里面
         * in:3> SensorReading{id='sensor_1', timestamp=1599990790000, temperature=1.0}
         * late-data:2> SensorReading{id='sensor_1', timestamp=1599990790000, temperature=1.0}
         *
         *
         * 也可以参考
         * https://github.com/pandongning/myFlinkCode/blob/master/src/main/scala/flink/mystream/windows/t7_lateness/t7_lateness.scala
         */
        reduce.print("reduce");
        reduce.getSideOutput(lateOutputTag).print("late-data");

        env.execute("ReduceProcessFuncation");
    }
}
