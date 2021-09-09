package com.ab.flink.window;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-09 23:13
 **/
public class MyProcessWindowFunction extends ProcessWindowFunction<Tuple2<String ,Integer>,Integer,String , TimeWindow> {
    @Override
    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<Integer> out) throws Exception {
        int maxValue = Integer.MIN_VALUE;
        for (Tuple2<String, Integer> element : elements) {
            maxValue = Math.max(element.f1, maxValue);
        }
        out.collect(maxValue);

    }
}
