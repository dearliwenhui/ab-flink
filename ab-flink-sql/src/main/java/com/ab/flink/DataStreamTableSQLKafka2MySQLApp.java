package com.ab.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.catalog.Column;
import org.apache.flink.types.Row;

/**
 * @description: kafka 2 MySQL
 * @version: 0.0.1
 * @author: Dave.Li
 * @createTime: 2021-12-28 14:07
 **/
public class DataStreamTableSQLKafka2MySQLApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String kafkaConnector = "CREATE TABLE KafkaTable (" +
                "  `id` BIGINT," +
                "    name STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'flink'," +
                "  'properties.bootstrap.servers' = '106.75.171.152:9092'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")";
        tableEnv.executeSql(kafkaConnector);

        String jdbcConnector = "CREATE TABLE MyData  (" +
                "    id BIGINT," +
                "    name STRING" +
                ") WITH (" +
                "   'connector' = 'jdbc'," +
                "   'url' = 'jdbc:mysql://myhost:3306/flink'," +
                "   'table-name' = 'person'," +
                "    'username' = 'root'," +
                "    'password' = '1qaz!QAZ2wsx@WSX'" +
                ")";
        tableEnv.executeSql(jdbcConnector);


        tableEnv.executeSql("insert into MyData select id,name from KafkaTable");
//        Table table = tableEnv.sqlQuery("select id from KafkaTable");
//        tableEnv.toDataStream(table, Row.class).print("print:::");
//        env.execute("DataStreamTableSQLKafka2MySQLApp");
    }

}
