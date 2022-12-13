package com.pdn.apitest.utils;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Properties;

public  class ExecutionEnvironmentUtils {
    public static StreamExecutionEnvironment getExecutionEnvironment(){
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
        env.setParallelism(3);

       return env;
    }
}
