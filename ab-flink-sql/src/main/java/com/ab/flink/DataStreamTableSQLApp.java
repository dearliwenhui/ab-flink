package com.ab.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-10-07 21:18
 **/
public class DataStreamTableSQLApp {
    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        DataStreamSource<String> source = env.readTextFile("data/access.log");
        SingleOutputStreamOperator<Access> stream = source.map(value -> {
            String[] split = value.split(",");
            Access access = new Access();
            access.setTime(Long.parseLong(split[0]));
            access.setDomain(split[1]);
            access.setTraffic(Long.parseLong(split[2]));
            return access;
        }).returns(Types.POJO(Access.class));

        // DataStream ==> Table
        Table table = tableEnv.fromDataStream(stream);
        tableEnv.createTemporaryView("access",table);
        Table resultTable = tableEnv.sqlQuery("select * from access where domain = 'ab.com' ");
        // Table ==> DataStream
        tableEnv.toDataStream(resultTable, Row.class).print("row:");
        tableEnv.toAppendStream(resultTable, Row.class).print("row:");
        tableEnv.toDataStream(resultTable, Access.class).print("access:");
        tableEnv.toAppendStream(resultTable, Access.class).print("access:");

        /**
         * 进行聚合操作（例如sum）必须使用 toRetractStream 第一个字段boolean字段表示，ture：最新的数据，false：过期的数据
         */

        env.execute("DataStreamTableSQLApp");
    }
}
