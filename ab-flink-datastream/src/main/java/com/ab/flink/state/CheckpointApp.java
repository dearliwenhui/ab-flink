package com.ab.flink.state;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

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
        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        source.print();
        env.execute("CheckpointApp");
    }

}
