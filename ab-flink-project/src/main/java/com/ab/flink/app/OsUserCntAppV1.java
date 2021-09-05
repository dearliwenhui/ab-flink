package com.ab.flink.app;

import com.ab.flink.domain.Access;
import com.ab.flink.sink.MyRedisSink;
import com.ab.flink.source.AccessSourceFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.redis.RedisSink;
import org.apache.flink.streaming.connectors.redis.common.config.FlinkJedisPoolConfig;

/**
 * @description: 按照操作系统维度进行新老用户的统计分析,将统计的数据插入到redis
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-05 13:28
 **/
public class OsUserCntAppV1 {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        SingleOutputStreamOperator<Tuple3<String, Integer, Integer>> stream = env.addSource(new AccessSourceFunction())
                .filter(value -> "startup".equals(value.getEvent()))
                .map(new MapFunction<Access, Tuple3<String, Integer, Integer>>() {
                    @Override
                    public Tuple3<String, Integer, Integer> map(Access value) throws Exception {
                        return Tuple3.of(value.getOs(), value.getNu(), 1);
                    }
                }).keyBy(new KeySelector<Tuple3<String, Integer, Integer>, Tuple2<String, Integer>>() {
                    @Override
                    public Tuple2<String, Integer> getKey(Tuple3<String, Integer, Integer> value) throws Exception {
                        return Tuple2.of(value.f0, value.f1);
                    }
                }).sum(2);
        FlinkJedisPoolConfig conf = new FlinkJedisPoolConfig.Builder().setHost("myhost").setPort(6379).setPassword("redis").build();
        stream.addSink(new RedisSink<Tuple3<String, Integer, Integer>>(conf, new MyRedisSink()));
        env.execute("OsUserCntAppV1");

    }
}
