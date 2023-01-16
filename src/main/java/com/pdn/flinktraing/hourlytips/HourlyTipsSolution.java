package com.pdn.flinktraing.hourlytips;

import com.pdn.apitest.utils.ExecutionEnvironmentUtils;
import com.pdn.apitest.utils.KafkaUtils;
import com.pdn.flinktraing.beans.TaxiFare;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.scala.ExecutionEnvironment;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.IOException;
import java.math.BigInteger;
import java.time.Duration;

/**
 * @author zhihu
 */
public class HourlyTipsSolution {
    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = ExecutionEnvironmentUtils.getExecutionEnvironment();
        FlinkKafkaConsumer011<String> source = KafkaUtils.getFlinkKafkaConsumer011("HourlyTipsSolution", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "a");

        DataStreamSource<String> inputStream = env.addSource(source);
        SingleOutputStreamOperator<TaxiFare> map = inputStream.map(line -> {
            String[] strings = line.split(",");
            if (strings.length == 3) {
                return new TaxiFare(strings[0], Float.parseFloat(strings[2]), Long.parseLong(strings[1]));
            } else {
                return null;
            }

        });

        SingleOutputStreamOperator<Tuple3<String, Float, Long>> hourlyTips = map.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.getTimeStamp())
                                .withIdleness(Duration.ofSeconds(4)))
                .keyBy(TaxiFare::getDriverId)
                .timeWindow(Time.seconds(5))
                .process(new AddTips());

        SingleOutputStreamOperator<Tuple3<String, Float, Long>> hourlyMax = hourlyTips.timeWindowAll(Time.seconds(5))
                .maxBy(1);

        hourlyMax.print();

        env.execute("HourlyTipsSolution");


    }

    static class maxHourTips extends ProcessAllWindowFunction<Tuple3<String,Float,Long>,Tuple3<String,Float,Long>,TimeWindow>{

        @Override
        public void process(ProcessAllWindowFunction<Tuple3<String, Float, Long>, Tuple3<String, Float, Long>, TimeWindow>.Context context, Iterable<Tuple3<String, Float, Long>> elements, Collector<Tuple3<String, Float, Long>> out) throws Exception {
        }
    }

    static class  AddTips extends ProcessWindowFunction<TaxiFare, Tuple3<String,Float,Long>,String, TimeWindow> {

        @Override
        public void process(String key, ProcessWindowFunction<TaxiFare, Tuple3<String, Float, Long>, String, TimeWindow>.Context context, Iterable<TaxiFare> elements, Collector<Tuple3<String, Float, Long>> out) throws Exception {
            Float sumOfTips = 0F;
            for (TaxiFare element : elements) {
                sumOfTips+=element.getFares();
            }
            out.collect(Tuple3.of(key,sumOfTips,context.window().getEnd()));
        }
    }

}
