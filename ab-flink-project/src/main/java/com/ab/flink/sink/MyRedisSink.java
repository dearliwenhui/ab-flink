package com.ab.flink.sink;

import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-05 22:11
 **/
public class MyRedisSink implements RedisMapper<Tuple3<String,Integer,Integer>> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "os:un:cunt");
    }

    @Override
    public String getKeyFromData(Tuple3<String, Integer, Integer> data) {
        return data.f0 + data.f1;
    }

    @Override
    public String getValueFromData(Tuple3<String, Integer, Integer> data) {
        return String.valueOf(data.f2);
    }

}
