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
        hourlyTips.print("hourlyTips");

        SingleOutputStreamOperator<Tuple3<String, Float, Long>> hourlyMax = hourlyTips.timeWindowAll(Time.seconds(5))
                .maxBy(1);

        hourlyMax.print("hourlyMax");

        env.execute("HourlyTipsSolution");


    }

    /**
     * 输入数据
     * sensor_1,1599990791000,1
     * sensor_1,1599990791000,1.1
     * sensor_1,1599990791000,1.2
     * sensor_2,1599990791000,2
     * sensor_2,1599990791000,2.1
     * sensor_2,1599990791000,2.2
     * sensor_2a,1599990791000,2.1
     * sensor_2a,1599990791000,2.1
     * sensor_2a,1599990791000,2.2
     * sensor_3,1599990795000,3
     * sensor_3,1599990795000,3.1
     * sensor_3,1599990795000,3.2
     * sensor_4,1599990796000,4
     * sensor_4,1599990796000,4.1
     * sensor_4,1599990796000,4.2
     * sensor_5,1599990797000,5
     * sensor_6,1599990798000,6
     * sensor_7,1599990799000,7
     * sensor_8,1599990801000,8
     * sensor_9,1599990802000,9
     * sensor_10,1599990803000,10
     * sensor_11,1599990804000,11
     * 输出数据
     * hourlyTips:2> (sensor_1,3.3,1599990795000)
     * hourlyTips:3> (sensor_4,12.3,1599990800000)
     * hourlyTips:1> (sensor_2,6.3,1599990795000)
     * hourlyTips:3> (sensor_7,7.0,1599990800000)
     * hourlyTips:2> (sensor_5,5.0,1599990800000)
     * hourlyTips:1> (sensor_2a,6.3999996,1599990795000)
     * hourlyTips:2> (sensor_6,6.0,1599990800000)
     * hourlyTips:1> (sensor_3,9.3,1599990800000)
     * hourlyMax:1> (sensor_2a,6.3999996,1599990795000)
     * hourlyMax:2> (sensor_4,12.3,1599990800000)
     */

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
