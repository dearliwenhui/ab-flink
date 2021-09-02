package com.ab.flink.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

/**
 * @description:
 * @version: 0.0.1
 * @author: Dave.Li
 * @createTime: 2021-09-02 16:52
 **/
public class MyRichMapFunction extends RichMapFunction<String, Access> {
    @Override
    public void open(Configuration parameters) throws Exception {
        System.out.println("~~~~~open");
//        super.open(parameters);
    }

    @Override
    public void close() throws Exception {
        System.out.println("-------close");
//        super.close();
    }

    @Override
    public RuntimeContext getRuntimeContext() {
        return super.getRuntimeContext();
    }

    @Override
    public Access map(String value) throws Exception {
        System.out.println("!!!!!map");
        String[] split = value.split(",");
        Access access = new Access();
        access.setTime(Long.parseLong(split[0]));
        access.setDomain(split[1]);
        access.setTraffic(Long.parseLong(split[2]));
        return access;
    }
}
