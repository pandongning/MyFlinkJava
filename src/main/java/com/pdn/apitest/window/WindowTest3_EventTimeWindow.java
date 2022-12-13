package com.pdn.apitest.window;

import com.pdn.apitest.beans.SensorReading;
import org.apache.flink.api.common.eventtime.*;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.OutputTag;

import java.time.Duration;
import java.util.Properties;

/**
 * @ClassName: WindowTest3_EventTimeWindow
 * @Description:
 * @Author: pdn on 2020/11/10 9:33
 * @Version: 1.0
 */
public class WindowTest3_EventTimeWindow {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.getConfig().setAutoWatermarkInterval(100);


        // socket文本流
//        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

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
        DataStream<String> inputStream = env.addSource(new FlinkKafkaConsumer011<String>("a", new SimpleStringSchema(), properties));

        // 转换成SensorReading类型，分配时间戳和watermark
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
                    String[] fields = line.split(",");
                    return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
                })
                // 升序数据设置事件时间和watermark
//                .assignTimestampsAndWatermarks(new AscendingTimestampExtractor<SensorReading>() {
//                    @Override
//                    public long extractAscendingTimestamp(SensorReading element) {
//                        return element.getTimestamp() * 1000L;
//                    }
//                })
                // 乱序数据设置时间戳和watermark
//                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
//                    @Override
//                    public long extractTimestamp(SensorReading element) {
//                        return element.getTimestamp() * 1000L;
//                    }
//                });
                .assignTimestampsAndWatermarks(WatermarkStrategy.<SensorReading>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.getTimestamp())
//                        如果数据源中的某一个分区分片在一段时间内未发送事件数据，则意味着 WatermarkGenerator 也不会获得任何新数据去生成 watermark。
//                        我们称这类数据源为空闲输入或空闲源。在这种情况下，当某些其他分区仍然发送事件数据的时候就会出现问题。
//                        由于下游算子 watermark 的计算方式是取所有不同的上游并行数据源 watermark 的最小值，则其 watermark 将不会发生变化。
//                        为了解决这个问题，你可以使用 WatermarkStrategy 来检测空闲输入并将其标记为空闲状态。WatermarkStrategy 为此提供了一个工具接口：
                                .withIdleness(Duration.ofSeconds(10))
                );

        OutputTag<SensorReading> outputTag = new OutputTag<SensorReading>("late") {
        };

        // 基于事件时间的开窗聚合，统计15秒内温度的最小值
        SingleOutputStreamOperator<SensorReading> reduceTempStream = dataStream.keyBy("id")
                .timeWindow(Time.seconds(5))
                .allowedLateness(Time.seconds(1))
                .sideOutputLateData(outputTag)
                .reduce(new ReduceFunction<SensorReading>() {
                    @Override
                    public SensorReading reduce(SensorReading value1, SensorReading value2) throws Exception {
                        return new SensorReading(value1.getId(), value1.getTimestamp(), value1.getTemperature() + value2.getTemperature());
                    }
                });

        reduceTempStream.print("reduceTempStream");
        reduceTempStream.getSideOutput(outputTag).print("late");

        env.execute("WindowTest3_EventTimeWindow");
    }
}

class MyWatermarkStrategy implements WatermarkStrategy<SensorReading> {

    @Override
    public WatermarkGenerator<SensorReading> createWatermarkGenerator(WatermarkGeneratorSupplier.Context context) {
        return new MyWatermarkGenerator();
    }

    @Override
    public TimestampAssigner<SensorReading> createTimestampAssigner(TimestampAssignerSupplier.Context context) {
        return null;
    }
}

class MyWatermarkGenerator implements WatermarkGenerator<SensorReading> {

    private final long maxOutOfOrderness = 3000L;
    private long currentMaxTimestamp;

    @Override
    public void onEvent(SensorReading event, long eventTimestamp, WatermarkOutput output) {
        currentMaxTimestamp = Math.max(currentMaxTimestamp, eventTimestamp);
    }

    @Override
    public void onPeriodicEmit(WatermarkOutput output) {
        Watermark watermark = new Watermark(currentMaxTimestamp - maxOutOfOrderness - 1);
        output.emitWatermark(watermark);
    }
}
