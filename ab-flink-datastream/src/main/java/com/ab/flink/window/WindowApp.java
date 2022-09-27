package com.ab.flink.window;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * @description:
 * @version: 0.0.1
 * @author: Dave.Li
 * @createTime: 2021-09-07 16:33
 **/
public class WindowApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test05(env);
        env.execute("WindowApp");
    }

    /**
     * define window function for calc max value
     * 将数据放到buff中,最后输出
     */
    public static void test05(StreamExecutionEnvironment env) {
        env.socketTextStream("myhost", 9527)
                .map(value -> Tuple2.of("a", Integer.valueOf(value)))
                .returns(Types.TUPLE(Types.STRING,Types.INT)).rescale()
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .process(new MyProcessWindowFunction())
                .print();
    }


    /**
     * reduce 求和
     * @param env
     */
    public static void test04(StreamExecutionEnvironment env) {
        env.socketTextStream("myhost", 9527)
                .map(value -> {
                    String[] split = value.split(",");
                    return Tuple2.of(split[0], Integer.valueOf(split[1]));
                }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .reduce((value1, value2) -> Tuple2.of(value1.f0, value1.f1 + value2.f1)).print();

    }

    /**
     * 不分组，老版本运行方式 < 1.12
     * @param env
     */
    public static void test01(StreamExecutionEnvironment env) {
        env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime);
        DataStreamSource<String> streamSource = env.socketTextStream("myhost", 9527);
        streamSource.map((MapFunction<String, Integer>) value -> Integer.parseInt(value))
                .returns(TypeInformation.of(Integer.class))
                .timeWindowAll(Time.seconds(5))
                .sum(0)
                .print();
    }

    /**
     * 不分组，新版本运行方式 >= 1.12
     * @param env
     */
    public static void test02(StreamExecutionEnvironment env) {
        DataStreamSource<String> streamSource = env.socketTextStream("myhost", 9527);
        streamSource.map((MapFunction<String, Integer>) value -> Integer.parseInt(value))
                .returns(TypeInformation.of(Integer.class))
                .windowAll(TumblingProcessingTimeWindows.of(Time.seconds(5)))
                .sum(0)
                .print();
    }

    /**
     * 分组，新版本运行方式
     * @param env
     */
    public static void test03(StreamExecutionEnvironment env) {
        env.socketTextStream("myhost", 9527).map(value -> {
            String[] split = value.split(",");
            return Tuple2.of(split[0], Integer.parseInt(split[1]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy(value -> value.f0)
                .window(TumblingProcessingTimeWindows.of(Time.seconds(3)))
                .sum(1)
                .print();
    }
}
