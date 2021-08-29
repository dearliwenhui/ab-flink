package com.ab.flink.basic;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.util.Collector;

/**
 * @description: 第一个基于Flink实时处理快速入门案例
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-08-27 23:23
 **/
public class BatchWordCountApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment env = ExecutionEnvironment.getExecutionEnvironment();
        DataSource<String> source = env.readTextFile("data/WordCount");
        source.flatMap(new FlatMapFunction<String, String>() {
            @Override
            public void flatMap(String s, Collector<String> collector) throws Exception {
                String[] words = s.split(",");
                for (String word : words) {
                    collector.collect(word);
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return StringUtils.isNotEmpty(value);
            }
        }).map(new MapFunction<String, Tuple2<String,Integer>>() {
            @Override
            public Tuple2 map(String value) throws Exception {
                return new Tuple2(value, 1);
            }
        }).groupBy(0).sum(1).print();


    }

}
