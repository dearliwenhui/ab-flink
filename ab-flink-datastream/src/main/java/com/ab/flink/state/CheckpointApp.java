package com.ab.flink.state;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
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
        env.enableCheckpointing(5000);
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(5, TimeUnit.SECONDS)));
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        source.map(value -> {
            if ("no".equals(value)) {
                throw new RuntimeException("发生异常，消耗一次restartAttempt次数");
            }
            return value.toUpperCase();
        }).print();
        env.execute("CheckpointApp");
    }

}
