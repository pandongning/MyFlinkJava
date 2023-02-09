package com.pdn.wc;

import com.pdn.apitest.beans.Action;
import com.pdn.apitest.utils.ExecutionEnvironmentUtils;
import com.pdn.apitest.utils.KafkaUtils;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;

import java.io.IOException;

public class KafkaWordCount {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = ExecutionEnvironmentUtils.getExecutionEnvironment();

        // 输入数据类似"Alice", "login"    "Alice", "pay" 表示用户Alice先登陆，再进行了支付
        FlinkKafkaConsumer011<String> actionStreamConsumer = KafkaUtils.getFlinkKafkaConsumer011("actionStream", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "a");
        DataStream<String> actionStream = env.addSource(actionStreamConsumer).setParallelism(3);

        DataStream<Action> map = actionStream.map(line -> {
            Action action = new Action();
            try {
                String[] split = line.split(",");
                if (split.length == 2) {
                    action = new Action(split[0], split[1]);
                }
            } catch (Exception e) {
                System.out.println(line + "&&&&&");
            }
//            此处可以看出，对于map在数据异常的时候。其必须发出一个空的值
            return action;
        }).setParallelism(8).rebalance();


        map.map(Action::getAction).setParallelism(4);

//        SingleOutputStreamOperator<Action> flatMap = actionStream.flatMap(new FlatMapFunction<String, Action>() {
//            @Override
//            public void flatMap(String value, Collector<Action> out) throws Exception {
//                String[] split = value.split(",");
//                if (split.length == 2) {
//                    Action action = new Action(split[0], split[1]);
////                    对于flatMap在遇见异常数据的时候可以选择不发出数据
//                    out.collect(action);
//                }
//
//            }
//        });

//        flatMap.print("flatMap");

//        下面是简化的写法
//        actionStream.flatMap((FlatMapFunction<String, Action>) (value, out) -> {
//            String[] split = value.split(",");
//            if (split.length == 2){
//                Action  action = new Action(split[0], split[1]);
//                out.collect(action);
//            }
//
//        });


        env.execute("KafkaWordCount");
    }
}
