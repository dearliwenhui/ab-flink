package com.ab.flink.state;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.shaded.curator4.com.google.common.collect.Lists;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.UUID;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-12 15:25
 **/
public class StateApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        test01(env);
        env.execute("StateApp");
    }

    /**
     * 使用state计算平均数
     *
     * @param env
     */
    private static void test01(StreamExecutionEnvironment env) {
        ArrayList<Tuple2<Long, Long>> list = new ArrayList<>();
        list.add(Tuple2.of(1L, 2L));
        list.add(Tuple2.of(1L, 4L));
        list.add(Tuple2.of(2L, 2L));
        list.add(Tuple2.of(2L, 2L));
        list.add(Tuple2.of(1L, 8L));
        list.add(Tuple2.of(2L, 2L));
        env.fromCollection(list)
                .keyBy(value -> value.f0)
//                .flatMap(new AvgWithValueState())
                .flatMap(new AvgWithMapState())
                .print();

    }


}

class AvgWithMapState extends RichFlatMapFunction<Tuple2<Long,Long>,Tuple2<Long,Double>>{

    private transient MapState<String, Long> mapState;

    @Override
    public void open(Configuration parameters) throws Exception {
        MapStateDescriptor<String, Long> descriptor = new MapStateDescriptor<>("avg", String.class, Long.class);
        mapState = getRuntimeContext().getMapState(descriptor);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        mapState.put(UUID.randomUUID().toString(), value.f1);
        ArrayList<Long> elements = Lists.newArrayList(mapState.values());
        if (elements.size()==3) {
            Long count = 0L;
            Long sum = 0L;
            for (Long element : elements) {
                count++;
                sum += element;
            }
            Double average = sum / count.doubleValue();
            out.collect(Tuple2.of(value.f0, average));
            mapState.clear();
        }

    }
}

class AvgWithValueState extends RichFlatMapFunction<Tuple2<Long, Long>, Tuple2<Long, Double>> {

    //求平均数：记录条数，总和
    private transient ValueState<Tuple2<Long, Long>> sum;

    @Override
    public void open(Configuration parameters) throws Exception {
        //首先定义解释器
        ValueStateDescriptor<Tuple2<Long, Long>> avg = new ValueStateDescriptor<>("avg", Types.TUPLE(Types.LONG, Types.LONG));
        //从运行时上下文获取state
        sum = getRuntimeContext().getState(avg);
    }

    @Override
    public void flatMap(Tuple2<Long, Long> value, Collector<Tuple2<Long, Double>> out) throws Exception {
        //获取获取ValueState中的值
        Tuple2<Long, Long> value1 = sum.value();
        //判断获取的值是否是null
        if (value1 == null) {
            value1 = Tuple2.of(0L, 0L);
        }
        value1.f0 += 1;//获取次数
        value1.f1 += value.f1;//求和
        sum.update(value1);
        //数据大于等于3个时，求平均数
        if (value1.f0 >= 3) {
            out.collect(Tuple2.of(value.f0, value1.f1 / value1.f0.doubleValue()));
            //清理数据
            sum.clear();
        }
    }
}

