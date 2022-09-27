package com.ab.flink.newtrans;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-01-11 22:35
 **/
public class NewTransApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment executionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment();

        executionEnvironment.execute("NewTransApp");
    }



}