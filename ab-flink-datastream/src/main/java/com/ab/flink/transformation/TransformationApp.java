package com.ab.flink.transformation;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @version: 0.0.1
 * @author: Dave.Li
 * @createTime: 2021-09-01 16:51
 **/
public class TransformationApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        keyBy(env);
        env.execute("TransformationApp");
    }

    public static void keyBy(StreamExecutionEnvironment env) {
        DataStreamSource<String> streamSource = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> map = streamSource.map(new MapFunction<String, Access>() {
            @Override
            public Access map(String value) throws Exception {
                String[] split = value.split(",");
                Access access = new Access();
                access.setTime(Long.parseLong(split[0]));
                access.setDomain(split[1]);
                access.setTraffic(Long.parseLong(split[2]));
                return access;
            }
        });
        map.keyBy(value -> value.getDomain()).sum("traffic").print();

    }

    public static void flatmap(StreamExecutionEnvironment env) {
        DataStreamSource<String> streamSource = env.socketTextStream("myhost", 9527);
        SingleOutputStreamOperator<String> stringSingleOutputStreamOperator = streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] split = value.split(",");
                for (String s : split) {
                    out.collect(s);
                }
            }
        });
        stringSingleOutputStreamOperator.print();
    }

    public static void map(StreamExecutionEnvironment env) {
        DataStreamSource<String> streamSource = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> returns = streamSource.map((MapFunction<String, Access>) value -> {
            String[] split = value.split(",");
            Access access = new Access();
            access.setTime(Long.parseLong(split[0]));
            access.setDomain(split[1]);
            access.setTraffic(Long.parseLong(split[2]));
            return access;
        }).returns(TypeInformation.of(Access.class));
        returns.print();
    }

}
