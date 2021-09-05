package com.ab.flink.source;

import com.ab.flink.domain.Access;
import com.ab.flink.domain.Product;
import com.ab.flink.utils.MySQLUtils;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-05 18:10
 **/
public class AccessSourceFunction extends RichParallelSourceFunction<Access> {
    Connection connection;
    PreparedStatement pstm;


    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        connection = MySQLUtils.getConnection();
        pstm = connection.prepareStatement("select * from flink.access a, flink.product p where a.id = p.access_id limit 50 ");
    }

    @Override
    public void close() throws Exception {
        super.close();
        MySQLUtils.close(connection, pstm);

    }

    @Override
    public void run(SourceContext<Access> ctx) throws Exception {
        ResultSet resultSet = pstm.executeQuery();
        while (resultSet.next()) {
//            int id = resultSet.getInt("id");
            String device = resultSet.getString("device");
            String deviceType = resultSet.getString("deviceType");
            String os = resultSet.getString("os");
            String event = resultSet.getString("event");
            String net = resultSet.getString("net");
            String channel = resultSet.getString("channel");
            String uid = resultSet.getString("uid");
            int nu = resultSet.getInt("nu");
            String ip = resultSet.getString("ip");
            Long time = resultSet.getLong("time");
            String version = resultSet.getString("version");
            Access access = new Access();
            access.setDevice(device);
            access.setDeviceType(deviceType);
            access.setOs(os);
            access.setEvent(event);
            access.setNet(net);
            access.setChannel(channel);
            access.setUid(uid);
            access.setNu(nu);
            access.setIp(ip);
            access.setTime(time);
            access.setVersion(version);
            Product product = new Product();
            product.setCategory(resultSet.getString("category"));
            product.setName(resultSet.getString("name"));
            access.setProduct(product);
            ctx.collect(access);
        }
    }

    @Override
    public void cancel() {

    }
}


