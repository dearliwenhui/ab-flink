package com.ab.flink.sink;

import com.ab.flink.source.AccessSource;
import com.ab.flink.source.ParallAccessSource;
import com.ab.flink.transformation.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.tuple.Tuple2;
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
        DataStreamSource<Access> source = env.addSource(new AccessSource());
        source.map(new MapFunction<Access, Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> map(Access value) throws Exception {
                return Tuple2.of(value.getDomain(), value.getTraffic());
            }
        }).keyBy(value -> value.f0).reduce(new ReduceFunction<Tuple2<String, Long>>() {
            @Override
            public Tuple2<String, Long> reduce(Tuple2<String, Long> value1, Tuple2<String, Long> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }).addSink(new MySQLSink());
        env.execute("SinkApp");
    }
}
