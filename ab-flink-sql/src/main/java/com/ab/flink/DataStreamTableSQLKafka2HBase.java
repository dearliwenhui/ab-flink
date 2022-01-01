package com.ab.flink;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

/**
 * @description: kafka 2 MySQL
 * @version: 0.0.1
 * @author: Dave.Li
 * @createTime: 2021-12-28 14:07
 **/
public class DataStreamTableSQLKafka2HBase {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
        String kafkaConnector="CREATE TABLE KafkaTable (" +
                "  `id` STRING" +
                ") WITH (" +
                "  'connector' = 'kafka'," +
                "  'topic' = 'RTS_WTCTW_REFUND'," +
                "  'properties.bootstrap.servers' = '10.95.35.76:33100,10.95.35.77:33102,10.95.35.78:33101'," +
                "  'properties.group.id' = 'testGroup'," +
                "  'scan.startup.mode' = 'earliest-offset'," +
                "  'format' = 'json'" +
                ")";
        tableEnv.executeSql(kafkaConnector);

        String hbaseConnector = "CREATE TABLE hTable (" +
                " ID VARCHAR ," +
                " PRIMARY KEY (ID) NOT ENFORCED " +
                ") WITH (" +
                " 'connector' = 'hbase-1.4'," +
                " 'table-name' = 'DEV_RTS_STOCK_PNSHK.TEST'," +
                " 'zookeeper.quorum' = '10.95.35.160:2181'" +
                ")";
        tableEnv.executeSql(hbaseConnector);


        tableEnv.executeSql("insert into hTable select id from KafkaTable");
//        Table table = tableEnv.sqlQuery("select id from KafkaTable");
//        tableEnv.toDataStream(table, Row.class).print("print:::");
//        env.execute("DataStreamTableSQLKafka2MySQLApp");
    }

}
