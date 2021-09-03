package com.ab.flink.source;

import com.mysql.jdbc.Connection;
import com.mysql.jdbc.PreparedStatement;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-02 23:05
 **/
public class StuSource extends RichSourceFunction {

    Connection connection;
    PreparedStatement psmt;

    @Override
    public void run(SourceContext ctx) throws Exception {
        System.out.println();
    }

    @Override
    public void cancel() {

    }
}
