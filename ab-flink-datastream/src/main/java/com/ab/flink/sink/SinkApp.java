package com.ab.flink.sink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @version: 0.0.1
 * @author: Dave.Li
 * @createTime: 2021-09-02 11:01
 **/
public class SinkApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> streamSource = env.socketTextStream("myhost", 9527);
        streamSource.print();
        env.execute("SinkApp");
    }
}
