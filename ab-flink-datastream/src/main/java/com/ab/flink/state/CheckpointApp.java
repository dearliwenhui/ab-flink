package com.ab.flink.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.state.hashmap.HashMapStateBackend;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-21 20:23
 **/
public class CheckpointApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        //state存储的时间
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));
        CheckpointConfig config = env.getCheckpointConfig();
        //当job是关闭时，保留checkpoint，如果不设置，当job关闭，checkpoint会被清空
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //使用配置文件设置存储路径
        //设置state快照存储路径
//        env.setStateBackend(new HashMapStateBackend());
//        env.getCheckpointConfig().setCheckpointStorage("file:///checkpoint");

        DataStreamSource<String> source = env.socketTextStream("106.75.171.152", 9527);
        source.map(value -> {
            if ("no".equals(value)) {
                throw new RuntimeException("发生异常，消耗一次restartAttempt次数");
            }
            return value.toUpperCase();
        })

                .map(value -> Tuple2.of(value, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0).sum(1)
                .print();
        env.execute("CheckpointApp");
    }

}
