package com.ab.flink.newtrans;

import org.apache.flink.streaming.api.functions.source.RichParallelSourceFunction;

import java.util.Map;

/**
 * @description: 使用MySQL自定义source
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2022-01-16 15:10
 **/
public class MySQLSource extends RichParallelSourceFunction<Map> {



    @Override
    public void run(SourceContext<Map> ctx) throws Exception {

    }

    @Override
    public void cancel() {

    }
}
