package com.pdn.apitest.utils;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.runtime.state.memory.MemoryStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

public class ExecutionEnvironmentUtils {

    /**
     * 重启的次数.实际的生产环境该参数为99次。
     * 此处为了测试先写成3
     */
    private static final int RESTART_ATTEMPTS = 3;
    //    private static int  DELAY_BETWEEN_ATTEMPTS  = 30000;
    private static final Time DELAY_BETWEEN_ATTEMPTS = Time.of(30, TimeUnit.SECONDS);
    /**
     * 每隔10min做一次ck 「600000」
     * 此处为了测试，设置为5s 「5000」
     */
    private static final int CHECKPOINT_INTERVAL = 10000;

    /**
     * checkpoint 超时时长 5min 「30000」
     * 此处为了测试，设置为 2min「120000」
     */
    private static final int CHECKPOINT_TIMEOUT = 120000;

    /**
     * 两次 checkpoint 最小时间间隔
     */
    private static final int MIN_PAUSE_CHECKPOINT_INTERVAL = 20000;

    /**
     * 同时做 checkpoint 的并发度
     */
    private static final int MAX_CONCURRENT_CHECKPOINTS = 3;

    public static StreamExecutionEnvironment getExecutionEnvironment() throws IOException {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

//        ck的设置
        env.enableCheckpointing(CHECKPOINT_INTERVAL);
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        env.getCheckpointConfig().setCheckpointTimeout(CHECKPOINT_TIMEOUT);
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(MIN_PAUSE_CHECKPOINT_INTERVAL);
//        并发 checkpoint 的数目: 默认情况下，在上一个 checkpoint 未完成（失败或者成功）的情况下，系统不会触发另一个 checkpoint。这确保了拓扑不会在 checkpoint 上花费太多时间，从而影响正常的处理流程。 不过允许多个 checkpoint 并行进行是可行的，对于有确定的处理延迟（例如某方法所调用比较耗时的外部服务），但是仍然想进行频繁的 checkpoint 去最小化故障后重跑的 pipelines 来说，是有意义的。
//该选项不能和 “checkpoints 间的最小时间"同时使用。
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(MAX_CONCURRENT_CHECKPOINTS);
//        设置ck允许失败的次数
//         允许两个连续的 checkpoint 错误
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);


//           //     设置ck的StateBackend
//        FsStateBackend fsStateBackend = new FsStateBackend("hdfs://localhost:9000/flink/file/checkpoints");
//       // 如果在idea本地配置的存储路径是hdfs，而且集群在docker里面，则此时是不能在本地测试的，因为此时的时候需要很多的端口
//        RocksDBStateBackend rocksDBStateBackend = new RocksDBStateBackend("hdfs://localhost:9000/flink/file/checkpoints/rocksDB");
//        MemoryStateBackend hashMapStateBackend = new MemoryStateBackend();
//        env.setStateBackend(rocksDBStateBackend);


        /**
         * 设置重启的策略
         * Flink 通过重启策略和故障恢复策略来控制 Task 重启：重启策略决定是否可以重启以及重启的间隔；故障恢复策略决定哪些 Task 需要重启。
         * 具体的参考文档地址
         * https://nightlies.apache.org/flink/flink-docs-release-1.16/zh/docs/ops/state/task_failure_recovery/#task-%e6%95%85%e9%9a%9c%e6%81%a2%e5%a4%8d
         *
         * 如果我们向Kafka里面输入了脏数据，则当前的程序会失败，然后重启，但是重启是从上次ck的位置开始重启的，所以其重启之后会继续消费该脏数据。所以又回再次失败
         * 直到达到配置的重启次数。所以重启针对的是集群出现问题的重启，其可以帮助我们自动的恢复程序。对于出现的脏数据，则需要自己主动的在程序里面处理
         *
         *
         * 固定延时重启策略按照给定的次数尝试重启作业。 如果尝试超过了给定的最大次数，作业将最终失败。 在连续的两次重启尝试之间，重启策略等待一段固定长度的时间。
         *
         * 下面的意思是尝试重启三次，每次的间隔为30s。如果重启3次仍然失败。则判定该任务失败
         */
//        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(
//                RESTART_ATTEMPTS, // 尝试重启的次数.超过这个次数则判定任务失败。
//                DELAY_BETWEEN_ATTEMPTS) // 每次重启之间的时间间隔
//        );
//        env.setParallelism(3);

        return env;
    }
}
