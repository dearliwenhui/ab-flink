package com.ab.flink.transformation;

import org.apache.commons.math3.geometry.enclosing.EnclosingBall;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.ConnectedStreams;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
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
        coFlatmap(env);
        env.execute("TransformationApp");
    }

    public static void coFlatmap(StreamExecutionEnvironment env) {
        DataStreamSource<String> stream1 = env.fromElements("a b c", "e f g");
        DataStreamSource<String> stream2 = env.fromElements("1,2,3", "4,5,6");
        stream1.connect(stream2).flatMap(new CoFlatMapFunction<String, String, String>() {
            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                for (String s : value.split(" ")) {
                    out.collect(s);
                }
            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                for (String s : value.split(",")) {
                    out.collect(s);
                }
            }
        }).print();
    }

    public static void coMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> streamSource1 = env.socketTextStream("myhost", 9527);
        SingleOutputStreamOperator<Integer> stream2 = env.socketTextStream("myhost", 9528).map(new MapFunction<String, Integer>() {
            @Override
            public Integer map(String value) throws Exception {
                return Integer.valueOf(value);
            }
        });
        ConnectedStreams<String, Integer> connect = streamSource1.connect(stream2);
        connect.map(new CoMapFunction<String, Integer, String>() {

            @Override
            public String map1(String value) throws Exception {
                return value.toUpperCase();
            }

            @Override
            public String map2(Integer value) throws Exception {
                return String.valueOf(value * 10);
            }
        }).print();
    }

    public static void union(StreamExecutionEnvironment env) {
        DataStreamSource<String> streamSource1 = env.socketTextStream("myhost", 9527);
        streamSource1.union(streamSource1).print();
    }

    public static void richMap(StreamExecutionEnvironment env) {
        env.setParallelism(1);
        DataStreamSource<String> streamSource = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> map = streamSource.map(new MyRichMapFunction());
        map.print();
    }

    public static void reduce(StreamExecutionEnvironment env) {
        DataStreamSource<String> streamSource = env.socketTextStream("myhost", 9527);
        streamSource.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                for (String s : value.split(",")) {
                    out.collect(s);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(value -> value.f0)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }).print();
    }

    /**
     * 类似group by操作
     * @param env
     */
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
