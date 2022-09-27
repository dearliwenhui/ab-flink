package connector;

import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.descriptors.Csv;
import org.apache.flink.table.descriptors.FileSystem;
import org.apache.flink.table.descriptors.Schema;
import org.apache.flink.types.Row;

import static org.apache.flink.table.api.Expressions.$;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-10-18 23:17
 **/
public class ConnectorApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
//        DataStreamSource<String> source = env.readTextFile("data/access.log");
        tableEnv.connect(new FileSystem().path("data/access.log"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("timestamp", DataTypes.BIGINT())
                        .field("domain", DataTypes.STRING())
                        .field("traffic", DataTypes.DOUBLE())
                ).createTemporaryTable("access_ods");
        Table accessOds = tableEnv.from("access_ods");
        //聚合操作
        Table resultTable = accessOds.groupBy($("domain"))
                .aggregate($("traffic").sum().as("traffics"))
                .select($("domain"), $("traffics"));
        //聚合操作不能使用toAppendStream
        tableEnv.toRetractStream(resultTable, Row.class).print();

        //写出数据
        tableEnv.connect(new FileSystem().path("out"))
                .withFormat(new Csv())
                .withSchema(new Schema()
                        .field("domain", DataTypes.STRING())
                        .field("traffics", DataTypes.DOUBLE())
                ).createTemporaryTable("fileoutput");

        resultTable.executeInsert("fileoutput");

        env.execute("ConnectorApp");
    }

}
