package com.pdn.apitest.broadcast;

import com.pdn.apitest.beans.Action;
import com.pdn.apitest.beans.Pattern;
import com.pdn.apitest.utils.ExecutionEnvironmentUtils;
import com.pdn.apitest.utils.KafkaUtils;
import org.apache.flink.api.common.state.*;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastConnectedStream;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer011;
import org.apache.flink.util.Collector;
import scala.Tuple2;

import java.util.Map;

/**
 * @author pdn
 * 案例见知乎的DwsEduContentLastConsum
 * 官网的文档https://nightlies.apache.org/flink/flink-docs-release-1.15/zh/docs/dev/datastream/fault-tolerance/broadcast_state/
 * <p>
 * 接下来我们举一个广播状态的应用案例。考虑在电商应用中，往往需要判断用户先后发生的行为的“组合模式”，比如“登录-下单”或者“登录-支付”，检测出这些连续的行为进行统计，
 * 就可以了解平台的运用状况以及用户的行为习惯。
 */
public class BroadcastStateExample {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = ExecutionEnvironmentUtils.getExecutionEnvironment();

        // 输入数据类似"Alice", "login"    "Alice", "pay" 表示用户Alice先登陆，再进行了支付
        FlinkKafkaConsumer011<String> actionStreamConsumer = KafkaUtils.getFlinkKafkaConsumer011("actionStream", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "a");
        DataStream<String> actionStream = env.addSource(actionStreamConsumer).setParallelism(3);
        SingleOutputStreamOperator<Action> action = actionStream.map(x -> {
//            Action actionRe = new Action();
//            try {
//                String[] split = x.split(",");
//                actionRe = new Action(split[0], split[1]);
//                return actionRe;
//            }catch (Exception e){
//                System.out.println(x);
//            }
//
//            return actionRe;

            String[] split = x.split(",");
            return   new Action(split[0], split[1]);


        });


        /**
         * 输入的数据
         * "login", "pay"
         * 表示我们要将先登陆，再支付的用户。检测出来
         * 即"login", "pay"是我们的规则。
         * 但是将来我们估计还会继续添加其余的规则，比如"login", "buy"
         * 所以规则是需要用一个集合存储起来的，因为我们可能会有很多的规则
         */
        FlinkKafkaConsumer011<String> patternStreamConsumer = KafkaUtils.getFlinkKafkaConsumer011("patternStream", "127.0.0.1:9094,127.0.0.1:9092,127.0.0.1:9093", "b");
        DataStream<String> patternStream = env.addSource(patternStreamConsumer).setParallelism(4);
        SingleOutputStreamOperator<Pattern> pattern = patternStream.map(x -> {
            String[] split = x.split(",");
            return new Pattern(split[0], split[1]);
        });

        MapStateDescriptor<String, Pattern> patternMapStateDescriptor = new MapStateDescriptor<>("patterns", Types.STRING, Types.POJO(Pattern.class));
//        MapStateDescriptor<String, Pattern> patternsTwo = new MapStateDescriptor<>("a", TypeInformation.of(String.class), TypeInformation.of(Pattern.class));
//        MapStateDescriptor<String, Pattern> patternsThere = new MapStateDescriptor<>("a", Types.STRING,
//                TypeInformation.of(new TypeHint<Pattern>() {}));
//        将规则数据进行广播。广播之后的规则其实是做为一个状态被发送到其余的task里面。所以此处需要一个MapStateDescriptor「描述该规则之后是如何存储到状态里面的」
        BroadcastStream<Pattern> broadcastPattern = pattern.broadcast(patternMapStateDescriptor);

//        因为是要检测一个用户是否先登陆，再进行注册，所以此处需要按照userid进行分组
        BroadcastConnectedStream<Action, Pattern> connectedStream = action.keyBy(Action::getUserId).connect(broadcastPattern);

        SingleOutputStreamOperator<Tuple2<String, Pattern>> process = connectedStream.process(new PatternEvaluator());

        process.print("rule");

        env.execute("BroadcastStateExample");
    }
}

class PatternEvaluator extends KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>> {

    ValueState<String> prevActionState;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        prevActionState = getRuntimeContext().getState(new ValueStateDescriptor<String>("lastAction", Types.STRING));
    }

    @Override
    public void processElement(Action value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.ReadOnlyContext ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
        //  在 getBroadcastState() 方法中传入的 stateDescriptor 应该与调用 .broadcast(patternMapStateDescriptor) 的参数相同。只有参数相同了，才能获得被广播出去的规则流
//       下面是得到所有的规则
        ReadOnlyBroadcastState<String, Pattern> patterns = ctx.getBroadcastState(new MapStateDescriptor<>("patterns", Types.STRING, Types.POJO(Pattern.class)));

        String prevAction = prevActionState.value();
        String curreAction = value.getAction();
        if (prevAction != null && curreAction != null) {
//            得到所有的规则.
            Iterable<Map.Entry<String, Pattern>> entries = patterns.immutableEntries();
//            循环所有的规则。检测目前用户的Action是否满足其中的某一个rule
            for (Map.Entry<String, Pattern> entry : entries) {
                String ruleName = entry.getKey();
                Pattern rule = entry.getValue();
                String action2 = rule.action2;
//                检测到匹配，则输出
                if (curreAction.equals(action2)) {
                    out.collect(Tuple2.apply(value.getUserId(), rule));
                } else {
//                    如果没有匹配，则需要更新
                    prevActionState.update(curreAction);
                }

            }
        }

//      如果用户是第一次来的，则给prevActionState赋予值
        if (prevAction == null) {
            prevActionState.update(curreAction);
        }
    }

    @Override
    public void processBroadcastElement(Pattern value, KeyedBroadcastProcessFunction<String, Action, Pattern, Tuple2<String, Pattern>>.Context ctx, Collector<Tuple2<String, Pattern>> out) throws Exception {
//       在 getBroadcastState() 方法中传入的 stateDescriptor 应该与调用 .broadcast(patternMapStateDescriptor) 的参数相同。
//       用于获得被广播出去的流
        BroadcastState<String, Pattern> patterns = ctx.getBroadcastState(new MapStateDescriptor<>("patterns", Types.STRING, Types.POJO(Pattern.class)));
//       将规则的2个字段拼接起来，做为规则的名字。
//       此处是将所有的规则放在map里面，如果规则流输入一条新的规则，则此时map里面就会多一条数据。因为其是在对广播状态进行操作，所以其操作会被其他的suatask立刻感知到
        patterns.put(value.getAction1() + "\t" + value.getAction2(), value);
    }
}

