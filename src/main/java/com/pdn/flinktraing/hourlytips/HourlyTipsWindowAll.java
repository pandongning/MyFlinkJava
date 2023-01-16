package com.pdn.flinktraing.hourlytips;

import com.pdn.apitest.utils.ExecutionEnvironmentUtils;
import com.pdn.apitest.utils.KafkaUtils;
import com.pdn.flinktraing.beans.TaxiFare;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessAllWindowFunction;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.HashMap;
import java.util.Set;

/**
 * @author zhihu
 */
public class HourlyTipsWindowAll {
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

        map.assignTimestampsAndWatermarks(
                        WatermarkStrategy.<TaxiFare>forBoundedOutOfOrderness(Duration.ofSeconds(2))
                                .withTimestampAssigner((event, timestamp) -> event.getTimeStamp())
                                .withIdleness(Duration.ofSeconds(4)))
                .timeWindowAll(Time.seconds(5))
               .process(new maxProcessAllWindowFunction())
                .print();

        env.execute("HourlyTipsWindowAll");

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
     * 输出数据
     * (sensor_2a,6.3999996,1599990795000)
     *
     * 继续输入
     * sensor_6,1599990798000,6
     * sensor_7,1599990799000,7
     * sensor_8,1599990801000,8
     * sensor_9,1599990802000,9
     * sensor_10,1599990803000,10
     * sensor_11,1599990804000,11
     * 输出
     * (sensor_4,12.3,1599990800000)
     */
    static class maxProcessAllWindowFunction extends ProcessAllWindowFunction<TaxiFare, Tuple3<String, Float, Long>, TimeWindow> {

        HashMap<String, TaxiFare> hashMap = new HashMap<>();

        @Override
        public void process(ProcessAllWindowFunction<TaxiFare, Tuple3<String, Float, Long>, TimeWindow>.Context context, Iterable<TaxiFare> elements, Collector<Tuple3<String, Float, Long>> out) throws Exception {
            for (TaxiFare next : elements) {
                TaxiFare taxiFare = hashMap.get(next.getDriverId());
                if (taxiFare != null) {
                    float fares = next.getFares();
                    taxiFare.setFares(taxiFare.getFares() + fares);
                }
                else {
                    hashMap.put(next.getDriverId(),next);
                }
            }

            float maxTaxiFare = 0;
            String maxKey = "NONE";

            Set<String> keys = hashMap.keySet();
            for (String key : keys) {
                TaxiFare taxiFare = hashMap.get(key);
                float fares = taxiFare.getFares();
                if (fares > maxTaxiFare){
                    maxTaxiFare = fares;
                    maxKey = key;
                }
            }

            out.collect(Tuple3.of(maxKey,hashMap.get(maxKey).getFares(),context.window().getEnd()));

        }
    }
}
