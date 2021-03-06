package com.atguigu.apitest.tableapi;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.java.BatchTableEnvironment;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.OldCsv;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest2_CommonApi
 * @Description:
 * @Author: pdn on 2020/11/13 10:29
 * @Version: 1.0
 */
public class TableTest2_CommonApi {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 1.1 基于老版本planner的流处理
        EnvironmentSettings oldStreamSettings = EnvironmentSettings.newInstance()
                .useOldPlanner()
                .inStreamingMode()
                .build();

        StreamTableEnvironment oldStreamTableEnv = StreamTableEnvironment.create(env, oldStreamSettings);

        // 1.2 基于老版本planner的批处理
        ExecutionEnvironment batchEnv = ExecutionEnvironment.getExecutionEnvironment();
        BatchTableEnvironment oldBatchTableEnv = BatchTableEnvironment.create(batchEnv);


        // 1.3 基于Blink的流处理
        EnvironmentSettings blinkStreamSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inStreamingMode()
                .build();
        StreamTableEnvironment blinkStreamTableEnv = StreamTableEnvironment.create(env, blinkStreamSettings);

        // 1.4 基于Blink的批处理
        EnvironmentSettings blinkBatchSettings = EnvironmentSettings.newInstance()
                .useBlinkPlanner()
                .inBatchMode()//指定是batch模式的
                .build();
        TableEnvironment blinkBatchTableEnv = TableEnvironment.create(blinkBatchSettings);

        // 2. 表的创建：连接外部系统，读取数据
        // 2.1 读取文件
//        String filePath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        String filePath = "E:\\教程\\大数据\\sggFlink（Java版）\\代码\\FlinkTutorial\\src\\main\\resources\\sensor.txt";
        tableEnv.connect(new FileSystem().path(filePath))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

//        创建一个table对象，则以后可以使用TableApi进行操作
        Table inputTable = tableEnv.from("inputTable");
//        inputTable.printSchema();
//        tableEnv.toAppendStream(inputTable, Row.class).print();

        // 3. 查询转换
        // 3.1 Table API
        // 简单转换
        Table resultTable = inputTable.select("id, temp")
                .filter("id === 'sensor_6'");

        // 聚合统计
        Table aggTable = inputTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        // 3.2 SQL
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'senosr_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 打印输出
//        tableEnv.toAppendStream(resultTable, Row.class).print("result");
//        下面必须修改为toRetractStream。因为此处进行了聚合的操作，对于流类型的应用。聚合操作，会更新以前的结果，所以需要使用toRetractStream
//        tableEnv.toRetractStream(aggTable, Row.class).print("agg");

        /**
         * 注意其对于一条数据会输出两条结果
         * 比如对于sensor_1
         * 第一条sensor_1的数据sensor_1,1547718199,35.8到来的时候
         * 输出结果如下
         * sqlagg> (true,sensor_1,1,35.8)
         * 第二条sensor_1的数据sensor_1,1547718207,36.3到来
         * 输出结果如下
         * 第一个false表示以前的结算结果。
         * 第二个true表示本次更新的结果
         * sqlagg> (false,sensor_1,1,35.8)
         * sqlagg> (true,sensor_1,2,36.05)
         */
        tableEnv.toRetractStream(sqlAggTable, Row.class).print("sqlagg");

        DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(sqlAggTable, Row.class);


        env.execute();
    }
}
