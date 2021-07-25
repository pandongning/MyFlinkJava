package com.atguigu.apitest.state;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Collections;
import java.util.List;

/**
 * @ClassName: StateTest1_OperatorState
 * @Description:
 * @Author: pdn on 2020/11/10 15:30
 * @Version: 1.0
 */
public class StateTest1_OperatorState {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        // socket文本流
        DataStream<String> inputStream = env.socketTextStream("localhost", 7777);

        // 转换成SensorReading类型
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        });

        // 定义一个有状态的map操作，统计当前分区数据个数
        SingleOutputStreamOperator<Integer> resultStream = dataStream.map(new MyCountMapper());

        resultStream.print();

        env.execute();
    }

    /**
     * 自定义MapFunction
     */
    public static class MyCountMapper implements MapFunction<SensorReading, Integer>, ListCheckpointed<Integer> {
        // 定义一个本地变量，作为算子状态
        private Integer count = 0;

        @Override
        public Integer map(SensorReading value) throws Exception {
            count++;
            return count;
        }


        /**
         * 自己定义的状态仅仅是一个本地的变量。需要将其置于下面的方法里面。
         * 则其可以被持久化保存起来，等到下次故障恢复的时候，再从检查点里面恢复出来
         *
         * @param checkpointId
         * @param timestamp
         * @return
         * @throws Exception
         */
        @Override
        public List<Integer> snapshotState(long checkpointId, long timestamp) throws Exception {
//            用于得到只包含一个元素的的集合。因为
            return Collections.singletonList(count);
        }

        /**
         * 从检查点里面恢复出来被保存的状态
         * 因为一个算子可能会有多个子任务，所以此时会将多个子任务的状态发送进行恢复，
         * 所以下面的
         *
         * @param state
         * @throws Exception
         */
        @Override
        public void restoreState(List<Integer> state) throws Exception {
            for (Integer num : state) {
                count += num;
            }
        }
    }
}
