package com.ab.flink.sink;

import com.ab.flink.transformation.Access;
import com.ab.flink.utils.MySQLUtils;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;

/**
 * @description: define MySQL sink
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-04 22:28
 **/
public class MySQLSink extends RichSinkFunction<Tuple2<String, Long>> {

    Connection connection;
    PreparedStatement insertPstm;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtils.getConnection();
        insertPstm = connection.prepareStatement("INSERT INTO `mall666`.`stu-flink` ( `name`, `age`) VALUES ( ?, ?)");
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (connection != null) connection.close();
        if (insertPstm != null) insertPstm.close();
    }

    @Override
    public void invoke(Tuple2<String, Long> value, Context context) throws Exception {
        insertPstm.setString(1, value.f0);
        insertPstm.setLong(2, value.f1);
        insertPstm.execute();
    }
}
