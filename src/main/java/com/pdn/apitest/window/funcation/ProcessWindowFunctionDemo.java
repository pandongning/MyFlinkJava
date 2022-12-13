package com.pdn.apitest.window.funcation;

import com.pdn.apitest.beans.SensorReading;
import com.pdn.apitest.utils.ExecutionEnvironmentUtils;
import com.pdn.apitest.utils.KafkaUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.WindowedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import scala.Tuple5;

import java.time.Duration;

public class ProcessWindowFunctionDemo {
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

        WindowedStream<SensorReading, String, TimeWindow> sensorReadingStringTimeWindowWindowedStream = dataStream
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//                        如果数据源中的某一个分区分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。
//                        我们称这类数据源为空闲输入或空闲源。在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。
//                        由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化。
//                        为了解决这个问题，你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。WatermarkStrategy 为此提供了一个工具接口：
                                .withIdleness(Duration.ofSeconds(10))
                )
                .keyBy(SensorReading::getId)
                .timeWindow(Time.seconds(5));

        sensorReadingStringTimeWindowWindowedStream.process(new MyProcessWindowFunction()).print("out");


        env.execute("ProcessWindowFunctionDemo");

    }
}

/**
 * 情况1输入
 * in:3> SensorReading{id='sensor_1', timestamp=1599990790000, temperature=1.0}
 * in:3> SensorReading{id='sensor_1', timestamp=1599990790000, temperature=1.1}
 * in:3> SensorReading{id='sensor_1', timestamp=1599990790000, temperature=1.2}
 * in:3> SensorReading{id='sensor_2', timestamp=1599990790000, temperature=2.0}
 * in:3> SensorReading{id='sensor_2', timestamp=1599990790000, temperature=2.1}
 * in:3> SensorReading{id='sensor_2', timestamp=1599990790000, temperature=2.2}
 * in:3> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.0}
 * in:3> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.1}
 * in:3> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.2}
 * in:1> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.0}
 * in:2> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.1}
 * in:1> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.2}
 * in:2> SensorReading{id='sensor_5', timestamp=1599990797000, temperature=5.0}
 * 下面是输出。虽然窗口的长度为5，即本来应该到了1599990795000，则可以输出数据了，但是因为此时带了2s的水印延迟，所以最后到了1599990797000才有输出
 * out:1> (sensor_2,1599990794999,1599990790000,1599990795000,6.3)
 * out:2> (sensor_1,1599990794999,1599990790000,1599990795000,3.3)
 *
 * 情况2输入
 * in:1> SensorReading{id='sensor_1', timestamp=1599990790000, temperature=1.0}
 * in:1> SensorReading{id='sensor_1', timestamp=1599990790000, temperature=1.1}
 * in:1> SensorReading{id='sensor_1', timestamp=1599990790000, temperature=1.2}
 * in:1> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.0}
 * in:1> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.1}
 * in:1> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.2}
 * in:1> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.0}
 * in:1> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.1}
 * in:1> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.2}
 * in:1> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.0}
 * in:1> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.1}
 * in:1> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.2}
 * in:2> SensorReading{id='sensor_5', timestamp=1599990797000, temperature=5.0}
 * out:1> (sensor_2,1599990794999,1599990790000,1599990795000,6.3)
 * 虽然sensor_2的开始时间为1599990791000。但是其key所在窗口的开始时间也是1599990790000。因为窗口的开始时间只和到达窗口的第一条时间有关系
 * out:2> (sensor_1,1599990794999,1599990790000,1599990795000,3.3)
 *
 *
 * 情况3输入
 * in:1> SensorReading{id='sensor_1', timestamp=1599990790001, temperature=1.0}
 * in:1> SensorReading{id='sensor_1', timestamp=1599990790002, temperature=1.1}
 * in:1> SensorReading{id='sensor_1', timestamp=1599990790003, temperature=1.2}
 * in:1> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.0}
 * in:1> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.1}
 * in:1> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.2}
 * in:1> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.0}
 * in:1> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.1}
 * in:1> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.2}
 * in:1> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.0}
 * in:1> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.1}
 * in:1> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.2}
 * in:1> SensorReading{id='sensor_5', timestamp=1599990797000, temperature=5.0}
 * out:1> (sensor_2,1599990794999,1599990790000,1599990795000,6.3)
 * 虽然sensor_1的开始时间为1599990790001。但是其窗口的开始时间还是被设置为1599990790000
 * out:2> (sensor_1,1599990794999,1599990790000,1599990795000,3.3)
 *
 *
 *
 * in:2> SensorReading{id='sensor_1', timestamp=5, temperature=1.0}
 * in:2> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.1}
 * in:2> SensorReading{id='sensor_1', timestamp=1599990791000, temperature=1.2}
 * in:2> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.0}
 * in:2> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.1}
 * in:2> SensorReading{id='sensor_2', timestamp=1599990791000, temperature=2.2}
 * in:2> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.0}
 * in:2> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.1}
 * in:2> SensorReading{id='sensor_3', timestamp=1599990795000, temperature=3.2}
 * in:2> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.0}
 * in:2> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.1}
 * in:2> SensorReading{id='sensor_4', timestamp=1599990796000, temperature=4.2}
 * in:2> SensorReading{id='sensor_5', timestamp=1599990797000, temperature=5.0}
 * 虽然sensor_1的第一条记录的时间为1599990791000。但是窗口的开始时间还是1599990790000
 * out:2> (sensor_1,1599990794999,1599990790000,1599990795000,3.3)
 * out:1> (sensor_2,1599990794999,1599990790000,1599990795000,6.3)
 *
 * 因为窗口的开始时间是根据下面的代码去计算的
 * return timestamp - (timestamp - offset + windowSize) % windowSize;
 * System.out.println(1599990791000L-(1599990791000L+5000)%5000)的值刚好就是1599990790000
 *
 */
class MyProcessWindowFunction extends ProcessWindowFunction<SensorReading, Tuple5<String, Long, Long, Long, Double>, String, TimeWindow> {

    @Override
    public void process(String key, ProcessWindowFunction<SensorReading, Tuple5<String, Long, Long, Long, Double>, String, TimeWindow>.Context context, Iterable<SensorReading> elements, Collector<Tuple5<String, Long, Long, Long, Double>> out) throws Exception {
        long start = context.window().getStart();
        long end = context.window().getEnd();
        long currentWatermark = context.currentWatermark();
        double temperatureTotal = 0.0;
        for (SensorReading element : elements) {
            temperatureTotal += element.getTemperature();
        }
        out.collect(new Tuple5<>(key, currentWatermark, start, end, temperatureTotal));
    }
}
