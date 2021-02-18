package com.atguigu.apitest.tableapi;

import com.atguigu.apitest.beans.SensorReading;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Over;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @ClassName: TableTest5_TimeAndWindow
 * @Description:
 * @Author: pdn on 2020/11/13 15:45
 * @Version: 1.0
 */
public class TableTest5_TimeAndWindow {
    public static void main(String[] args) throws Exception {
        // 1. 创建环境
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        // 2. 读入文件数据，得到DataStream
        DataStream<String> inputStream = env.readTextFile("E:\\教程\\大数据\\sggFlink（Java版）\\代码\\FlinkTutorial\\src\\main\\resources\\sensor.txt");

        // 3. 转换成POJO
        DataStream<SensorReading> dataStream = inputStream.map(line -> {
            String[] fields = line.split(",");
            return new SensorReading(fields[0], new Long(fields[1]), new Double(fields[2]));
        })
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<SensorReading>(Time.seconds(2)) {
                    @Override
                    public long extractTimestamp(SensorReading element) {
                        return element.getTimestamp() * 1000L;
                    }
                });

        // 4. 将流转换成表，定义时间特性
//processTime
        Table dataTable = tableEnv.fromDataStream(dataStream, "id, timestamp as ts, temperature as temp, pt.proctime");
//        dataTable.printSchema();
        /** 可以看出pt的类型为TIMESTAMP
         * root
         *  |-- id: STRING
         *  |-- ts: BIGINT
         *  |-- temp: DOUBLE
         *  |-- pt: TIMESTAMP(3) *PROCTIME*
         */

//        tableEnv.toAppendStream(dataTable, Row.class).print();
        /**
         * 可以看出其在输出的时候也是自动添加了时间字段
         * sensor_1,1547718199,35.8,2021-02-15 14:59:12.666
         * sensor_6,1547718201,15.4,2021-02-15 14:59:12.677
         * sensor_7,1547718202,6.7,2021-02-15 14:59:12.677
         */

        //eventTime版本1
//        Table dataTableEventTime = tableEnv.fromDataStream(dataStream, "id, timestamp.rowtime, temperature as temp");
//        dataTableEventTime.printSchema();
        /**
         * 可以看出timestamp的类型被转换为了TIMESTAMP(3) *ROWTIME*
         * 而在原始的数据里面其只是一个字符串1547718201
         * 所以此时其不再是原来的数据格式
         * root
         *  |-- id: STRING
         *  |-- timestamp: TIMESTAMP(3) *ROWTIME*
         *  |-- temp: DOUBLE
         */
//        tableEnv.toAppendStream(dataTableEventTime, Row.class).print();
        /**
         * 可以看到timestamp的输出被转换为了2019-01-17 09:43:19.0
         * sensor_1,2019-01-17 09:43:19.0,35.8
         * sensor_6,2019-01-17 09:43:21.0,15.4
         * sensor_7,2019-01-17 09:43:22.0,6.7
         */

//eventTime版本2
//        Table dataTableEventTime = tableEnv.fromDataStream(dataStream, "id, timestamp, temperature as temp, rt.rowtime");
//        dataTableEventTime.printSchema();
        /**
         * root
         *  |-- id: STRING
         *  |-- timestamp: BIGINT
         *  |-- temp: DOUBLE
         *  |-- rt: TIMESTAMP(3) *ROWTIME*
         */
//        tableEnv.toAppendStream(dataTableEventTime, Row.class).print();
        /**
         * 可以看出最后的一个字段rt就是timestamp对应的时间
         * sensor_1,1547718199,35.8,2019-01-17 09:43:19.0
         * sensor_6,1547718201,15.4,2019-01-17 09:43:21.0
         * sensor_7,1547718202,6.7,2019-01-17 09:43:22.0
         */


        tableEnv.createTemporaryView("sensor", dataTable);

        // 5. 窗口操作
        // 5.1 Group Window
        // table API
        Table resultTable = dataTable.window(Tumble.over("10.seconds").on("rt").as("tw"))
                .groupBy("id, tw")
                .select("id, id.count, temp.avg, tw.end");

        // SQL 注意末尾需要一个空格second) "
        Table resultSqlTable = tableEnv.sqlQuery("select id, count(id) as cnt, avg(temp) as avgTemp, tumble_end(rt, interval '10' second) " +
                "from sensor group by id, tumble(rt, interval '10' second)");


//        tableEnv.toAppendStream(resultTable, Row.class).print("result");
/**
 *         虽然上面使用了聚合函数，但是发现此处仍然可以使用toRetractStream流进行输出，关键原因在于其不会修改以前输出的结果
 *         第一：因为以前输出的结果属于上一个窗口，本次窗口里面虽然来了key相同的数据，但是其属于本次窗口，不会对上一个窗口里面的结果产生影响
 *         第二：本次窗口里面即使来了key相同的很多数据，但是其对外只输出一条结果，即本窗口里面也不存在更新已经发送出去的结果
 *         对于已经发送出去的结果如果需要进行更新，则此时不能使用toRetractStream
 */
//        tableEnv.toRetractStream(resultSqlTable, Row.class).print("sql");


        // 5.2 Over Window
        // table API
        Table overResult = dataTable.window(Over.partitionBy("id").orderBy("rt").preceding("2.rows").as("ow"))
                .select("id, rt, id.count over ow, temp.avg over ow");

        // SQL
        Table overSqlResult = tableEnv.sqlQuery("select id, rt, count(id) over ow, avg(temp) over ow " +
                " from sensor " +
                " window ow as (partition by id order by rt rows between 2 preceding and current row)");


        tableEnv.toAppendStream(overResult, Row.class).print("result");
        tableEnv.toRetractStream(overSqlResult, Row.class).print("sql");


        env.execute();
    }
}
