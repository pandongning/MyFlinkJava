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

public class AllowedLateness {
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

        final OutputTag<SensorReading> lateOutputTag = new OutputTag<SensorReading>("late-data"){};

        SingleOutputStreamOperator<Tuple3<SensorReading, String, Long>> reduce = dataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(1))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//                        如果数据源中的某一个分区分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。
//                        我们称这类数据源为空闲输入或空闲源。在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。
//                        由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化。
//                        为了解决这个问题，你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。WatermarkStrategy 为此提供了一个工具接口：
                                .withIdleness(Duration.ofSeconds(10))
                )
                .keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(2))
                .sideOutputLateData(lateOutputTag)
                .reduce((r1, r2) -> r1.getTemperature() > r2.getTemperature() ? r2 : r1,
                        new ProcessWindowFunction<SensorReading, Tuple3<SensorReading, String, Long>, String, TimeWindow>() {
                            @Override
                            public void process(String s, ProcessWindowFunction<SensorReading, Tuple3<SensorReading, String, Long>, String, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<Tuple3<SensorReading, String, Long>> out) throws Exception {
                                out.collect(new Tuple3<>(elements.iterator().next(), context.window().getStart() + "$$$" + context.window().getEnd(), context.currentWatermark()));
                            }
                        });

        /**
         * in:2> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.0}
         * in:2> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.1}
         * in:2> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.2}
         * in:2> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.0}
         * in:2> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.1}
         * in:2> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.2}
         * in:2> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.0}
         * in:2> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.1}
         * in:2> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.2}
         * in:2> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.0}
         * in:2> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.1}
         * in:2> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.2}
         * in:2> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.0}
         * in:2> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.1}
         * in:2> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.2}
         * in:2> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.0}
         * in:2> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.1}
         * in:2> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.2}
         * in:3> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.0}
         * 输入timestamp=1599990796000则会触发窗口的操作，因为窗口的长度为5s。窗口的的结束时间经过公式计算得到的值为1599990795000。
         * 但是因为WatermarkStrategy有1s的延迟「Duration.ofSeconds(1)」，
         * 所以只能等到1599990796000的时候才认为1599990795000的记录已经到达了，此时则可以触发窗口计算
         * reduce:1> (SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.0},1599990790000$$$1599990795000,1599990794999)
         * reduce:2> (SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.0},1599990790000$$$1599990795000,1599990794999)
         *
         * 此时输入延迟的数据
         * in:3> SensorReading{id='sensor_1', timestamp=1599990790000, temperature=-1.0}
         * 可以看出其更新了之前窗口输出的结果。因为我们在代码里面写了 .allowedLateness(Time.seconds(2))
         * reduce:2> (SensorReading{id='sensor_1', timestamp=1599990790000, temperature=-1.0},1599990790000$$$1599990795000,1599990794999)
         *
         * 继续推荐Watermark到7000-1s=6s
         * in:3> SensorReading{id='sensor_5', timestamp=1599990797000, temperature=5.0}
         *
         * 再次输入延迟的数据
         * 可以看出其更新了之前窗口输出的结果。因为我们在代码里面写了 .allowedLateness(Time.seconds(2))
         * in:1> SensorReading{id='sensor_2', timestamp=1599990790000, temperature=-2.0}
         * reduce:1> (SensorReading{id='sensor_2', timestamp=1599990790000, temperature=-2.0},1599990790000$$$1599990795000,1599990795999)
         *
         * 继续推进Watermark到8000-1s=7s
         * in:1> SensorReading{id='sensor_6', timestamp=1599990798000, temperature=6.0}
         *
         *
         * 再次输入延迟的数据。可以看到其是直接输出到延迟的流里面呢，而没有更新以前窗口的操作。因为.allowedLateness(Time.seconds(2))
         * 窗口是1599990795000时候结束的，加上延迟的2s，所以只要水印推进到了1599990797000。则下面来的数据会被输出到侧里面
         * in:1> SensorReading{id='sensor_3', timestamp=1599990790000, temperature=-3.0}
         * late-data:1> SensorReading{id='sensor_3', timestamp=1599990790000, temperature=-3.0}
         *
         *
         *
         */
        reduce.print("reduce");
        reduce.getSideOutput(lateOutputTag).print("late-data");

        env.execute("ReduceProcessFuncation");
    }
}
