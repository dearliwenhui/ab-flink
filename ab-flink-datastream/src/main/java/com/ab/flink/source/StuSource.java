package com.ab.flink.source;

import com.ab.flink.utils.MySQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-02 23:05
 **/
public class StuSource extends RichSourceFunction<Stu> {

    Connection connection;
    PreparedStatement psmt;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtils.getConnection();
        psmt = connection.prepareStatement("select * from stu limit 10");
    }

    @Override
    public void close() throws Exception {
        MySQLUtils.close(connection, psmt);
    }

    @Override
    public void run(SourceContext ctx) throws Exception {
        ResultSet resultSet = psmt.executeQuery();
        while (resultSet.next()) {
            String id = resultSet.getString("id");
            String name = resultSet.getString("name");
            String age = resultSet.getString("age");
            ctx.collect(new Stu(id, name, age));
        }

    }

    @Override
    public void cancel() {

    }
}
