package com.pdn.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.Kafka;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest4_KafkaPipeLine
 * @Description:
 * @Author: pdn on 2020/11/13 14:01
 * @Version: 1.0
 */
public class TableTest4_KafkaPipeLine {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 连接Kafka，读取数据
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sensor")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("id", DataTypes.STRING())
                        .field("timestamp", DataTypes.BIGINT())
                        .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("inputTable");

        // 3. 查询转换
        // 简单转换
        Table sensorTable = tableEnv.from("inputTable");
        Table resultTable = sensorTable.select("id, temp")
                .filter("id === 'sensor_6'");

        // 聚合统计
        Table aggTable = sensorTable.groupBy("id")
                .select("id, id.count as count, temp.avg as avgTemp");

        tableEnv.toRetractStream(aggTable,Row.class );

        // 4. 建立kafka连接，输出到不同的topic下
        tableEnv.connect(new Kafka()
                .version("0.11")
                .topic("sinktest")
                .property("zookeeper.connect", "localhost:2181")
                .property("bootstrap.servers", "localhost:9092")
        )
                .withFormat(new Csv())
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
//                        .field("timestamp", DataTypes.BIGINT())
                                .field("temp", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

//        注意向kafka里面写数据的时候也不能将聚合的结果写到kafka里面，即不能将aggTable的计算的结果写入到kafka里面
//        因为向kafka里面写数据的时候,其底层调用的是KafkaTableSinkBase.其实现了AppendStreamTableSink,所以其不也接受被更新的数据
//        此处自己猜想,是不是可以将将表转换为DataStream,然后再写入到kafka里面,但是此时则需要结合业务考虑结果的正确
        resultTable.insertInto("outputTable");



        env.execute();
    }
}
