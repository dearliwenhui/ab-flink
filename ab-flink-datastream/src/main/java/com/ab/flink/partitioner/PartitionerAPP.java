package com.ab.flink.partitioner;

import com.ab.flink.source.AccessSource;
import com.ab.flink.source.ParallAccessSource;
import com.ab.flink.transformation.Access;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-04 16:37
 **/
public class PartitionerAPP {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(3);
        DataStreamSource<Access> accessDataStreamSource = env.addSource(new ParallAccessSource());
        System.out.println(accessDataStreamSource.getParallelism());
        accessDataStreamSource.map(new MapFunction<Access, Tuple2<String, Access>>() {
            @Override
            public Tuple2<String, Access> map(Access value) throws Exception {
                return Tuple2.of(value.getDomain(), value);
            }
        }).partitionCustom(new MyPartitioner(), value -> value.f0).map(new MapFunction<Tuple2<String, Access>, Access>() {
            @Override
            public Access map(Tuple2<String, Access> value) throws Exception {
                System.out.println("current thread id is :" + Thread.currentThread().getId() + ",value"+value.f1);
                return value.f1;
            }
        }).print();
        env.execute("PartitionerAPP");
    }
}
