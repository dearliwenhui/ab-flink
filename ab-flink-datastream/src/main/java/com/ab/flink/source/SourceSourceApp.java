package com.ab.flink.source;

import com.ab.flink.transformation.Access;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Properties;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-08-30 22:41
 **/
public class SourceSourceApp {
    public static void main(String[] args) {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        defineStreamSource1(env);
    }

    public static void defineStreamSource1(StreamExecutionEnvironment env) {
        DataStreamSource<Access> accessDataStreamSource = env.addSource(new AccessSource());
        accessDataStreamSource.print();
    }

    public static void test3(StreamExecutionEnvironment env) {
        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers("myhost:9092")
                .setTopics("input-topic")
                .setGroupId("my-group")
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStreamSource<String> streamSource = env.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");
        System.out.println("kafka parallelism:" + streamSource.getParallelism());
        streamSource.print();


    }

    public static void test1(StreamExecutionEnvironment env) {
        DataStreamSource<String> source = env.socketTextStream("myhost", 9527);
        int sourceParallelism = source.getParallelism();//获取并行度
        System.out.println(sourceParallelism);
        SingleOutputStreamOperator<String> filter = source.filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !"a".equals(value);
            }
        });
        int parallelism = filter.getParallelism();
        System.out.println(parallelism);
    }

    public static void test2(StreamExecutionEnvironment env) {
        DataStreamSource<Long> source = env.fromParallelCollection(new NumberSequenceIterator(1, 10), Long.class);
        System.out.println("source:"+source.getParallelism());
        SingleOutputStreamOperator<Long> filter = source.filter(new FilterFunction<Long>() {
            @Override
            public boolean filter(Long value) throws Exception {
                return value > 5;
            }
        });
        System.out.println("filter:" + filter.getParallelism());
        filter.print();


    }
}
