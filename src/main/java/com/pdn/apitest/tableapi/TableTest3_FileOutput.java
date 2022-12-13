package com.pdn.apitest.tableapi;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;

/**
 * @ClassName: TableTest3_FileOutput
 * @Description:
 * @Author: wushengran on 2020/11/13 11:54
 * @Version: 1.0
 */
public class TableTest3_FileOutput {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 表的创建：连接外部系统，读取数据
        // 读取文件
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

        // 3.2 SQL 可以看出虽然进行的式sql查询，但是其返回的仍然是一个Table对象
        tableEnv.sqlQuery("select id, temp from inputTable where id = 'senosr_6'");
        Table sqlAggTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp from inputTable group by id");

        // 4. 输出到文件
        // 连接外部文件注册输出表
        String outputPath = "src/main/resources/out.txt";
        tableEnv.connect(new FileSystem().path(outputPath))
                .withFormat(new Csv())
//                下面是输出数据的字段信息
                .withSchema(new Schema()
                                .field("id", DataTypes.STRING())
//                        .field("cnt", DataTypes.BIGINT())
                                .field("temperature", DataTypes.DOUBLE())
                )
                .createTemporaryTable("outputTable");

        resultTable.insertInto("outputTable");
//        由于流式的聚合操作需要更新以前的结果，但是对于文件类型的sink，其只会追加，所以此时对于执行下面的操作，会爆出下面的错误提示
//        AppendStreamTableSink requires that Table has only insert changes.
        aggTable.insertInto("outputTable");

        env.execute();
    }
}
