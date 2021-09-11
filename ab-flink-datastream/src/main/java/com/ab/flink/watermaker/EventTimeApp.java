package com.ab.flink.watermaker;

import org.apache.commons.lang3.time.FastDateFormat;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

/**
 * @description: EventTime结合WM的使用
 * 输入数据的格式：时间字段,单词,次数
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-11 13:50
 **/
public class EventTimeApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test01(env);
        env.execute("EventTimeApp");
    }

    /**
     * 老版本实现方式,统计单词出现次数
     * @param env
     */
    private static void test01(StreamExecutionEnvironment env) {
        //侧流输出
        OutputTag<Tuple2<String, Integer>> outputTag = new OutputTag<Tuple2<String, Integer>>("late-output"){};
        SingleOutputStreamOperator<String> stream = env.socketTextStream("localhost", 9527)
                //延迟0秒执行，当时间>=4999是就会执行，如果是2，则时间是>=6999时，才会执行[0,4999)的数据
                .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<String>(Time.seconds(0)) {
                    @Override
                    public long extractTimestamp(String element) {
                        //获取EventTime
                        return Long.parseLong(element.split(",")[0]);
                    }
                });
        SingleOutputStreamOperator<Tuple2<String, Integer>> returns = stream.map(value -> {
            String[] split = value.split(",");
            return Tuple2.of(split[1], Integer.valueOf(split[2]));
        }).returns(Types.TUPLE(Types.STRING, Types.INT));
        //window对时间的精确到ms，
        SingleOutputStreamOperator<String> reduce = returns.keyBy(value -> value.f0)
                //第一个窗口是[0,4999)，第二个窗口是[5000,9999),如果延迟2s执行，当执行到[5000,6999)，又有[0,4999)之间的数据进来，依然会被计算
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .sideOutputLateData(outputTag)
                .reduce(new ReduceFunction<Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                        //增量打印，只要有相同的key增量时就会打印
                        System.out.println("------" + value1.f0 + " ==>" + (value1.f1 + value2.f1));
                        return Tuple2.of(value1.f0, value1.f1 + value2.f1);
                    }
                }, new ProcessWindowFunction<Tuple2<String, Integer>, String, String, TimeWindow>() {
                    FastDateFormat format = FastDateFormat.getInstance("yyyy-MM-dd HH:mm:ss");

                    @Override
                    public void process(String s, Context context, Iterable<Tuple2<String, Integer>> elements, Collector<String> out) throws Exception {
                        for (Tuple2<String, Integer> element : elements) {
                            //emit window start time and end time and element key and element value
                            //全量打印，只有全部运算完了，才会打印
                            System.out.println("Watermark 时间 ：" + format.format(context.currentWatermark()));
                            out.collect("[" + format.format(context.window().getStart()) + "==>" + context.window().getEnd() + "]," + element.f0 + "--->" + element.f1);

                        }
                    }
                });

        reduce.print();
        DataStream<Tuple2<String, Integer>> sideOutput = reduce.getSideOutput(outputTag);
        sideOutput.printToErr();

    }

}
