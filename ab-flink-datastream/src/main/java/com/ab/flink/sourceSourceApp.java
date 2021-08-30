package com.ab.flink;

import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-08-30 22:41
 **/
public class sourceSourceApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStreamSource<String> source = env.socketTextStream("myhost", 9527);
        int sourceParallelism = source.getParallelism();//获取并行度
        System.out.println(sourceParallelism);
        SingleOutputStreamOperator<String> filter = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !"a".equals(value);
            }
        });
        int parallelism = filter.getParallelism();
        System.out.println(parallelism);
    }
}
