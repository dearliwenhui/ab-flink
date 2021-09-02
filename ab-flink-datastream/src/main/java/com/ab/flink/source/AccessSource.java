package com.ab.flink.source;

import com.ab.flink.transformation.Access;
import org.apache.commons.lang3.RandomUtils;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.concurrent.TimeUnit;

/**
 * @description:
 * @version: 0.0.1
 * @author: Dave.Li
 * @createTime: 2021-09-02 17:38
 **/
public class AccessSource implements SourceFunction<Access> {
    @Override
    public void run(SourceContext<Access> ctx) throws Exception {
        String[] domains = {"a.com,b.com,c.com"};
        while (true) {
            for (int i = 0; i < 10; i++) {
                ctx.collect(new Access(RandomUtils.nextLong(100000, 999999), domains[RandomUtils.nextInt(0, 3)], RandomUtils.nextLong(100, 10000)));
            }
            TimeUnit.SECONDS.sleep(5);
        }
    }

    @Override
    public void cancel() {

    }
}
