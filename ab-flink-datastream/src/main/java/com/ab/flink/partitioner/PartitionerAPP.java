package com.ab.flink.partitioner;

import com.ab.flink.source.AccessSource;
import com.ab.flink.source.ParallAccessSource;
import com.ab.flink.transformation.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-04 16:37
 **/
public class PartitionerAPP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //开启checkpoint，每隔1000ms，往数据源中插入一个barrier
        //可以不用设置setCheckpointingMode
        env.enableCheckpointing(1000);
        //设置checkpoint状态后端，状态后端也就是数据存储位置
        env.setStateBackend(new HashMapStateBackend());
        env.getCheckpointConfig().setCheckpointStorage("hdfs://checkpoints");
        //设置容许checkpoint失败的次数
        env.getCheckpointConfig().setTolerableCheckpointFailureNumber(2);
        //设置checkpoint超时时间,5min
        env.getCheckpointConfig().setCheckpointTimeout(5 * 60 * 1000);
        //设置checkpoint模式，如果设置了enableCheckpointing(long interval)，则默认使用EXACTLY_ONCE
        env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        /**
         * 设置checkpoint任务之间的间隔时间，checkpoint job1 间隔 ms 后再checkpoint job2
         * 防止触发太密集的checkpoint任务，导致消耗过多的集群资源，导致影响整体性能
         * 默认限制setMaxConcurrentCheckpoints为1，可以不用设置setMaxConcurrentCheckpoints
         */
        env.getCheckpointConfig().setMinPauseBetweenCheckpoints(600);
        /**
         * 设置checkpoint最大并行的个数，如果设置setMinPauseBetweenCheckpoints，就只能设置这个值为1
         * 如果设置并行度>1，则setMinPauseBetweenCheckpoints不要设置
         */
        env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);
        //flink任务取消后，checkpoint数据是否删除
        //RETAIN_ON_CANCELLATION保留数据，DELETE_ON_CANCELLATION删除数据
        env.getCheckpointConfig().enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);


        env.execute("PartitionerAPP");
    }

    private void customPartition(StreamExecutionEnvironment env) {
        env.setParallelism(3);
        DataStreamSource<Access> accessDataStreamSource = env.addSource(new ParallAccessSource());
        System.out.println(accessDataStreamSource.getParallelism());
        accessDataStreamSource.map(new MapFunction<Access, Tuple2<String, Access>>() {
            @Override
            public Tuple2<String, Access> map(Access value) throws Exception {
                return Tuple2.of(value.getDomain(), value);
            }
        }).partitionCustom(new MyPartitioner(), value -> value.f0).map(new MapFunction<Tuple2<String, Access>, Access>() {
            @Override
            public Access map(Tuple2<String, Access> value) throws Exception {
                System.out.println("current thread id is :" + Thread.currentThread().getId() + ",value"+value.f1);
                return value.f1;
            }
        }).print();
    }
}
