package com.ab.flink.partitioner;

import org.apache.flink.api.common.functions.Partitioner;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-04 16:23
 **/
public class MyPartitioner implements Partitioner<String> {

    @Override
    public int partition(String key, int numPartitions) {
//        System.out.println("numPartitions:" + numPartitions);
        if ("a.com".equals(key)) {
            return 0;
        } else if ("b.com".equals(key)) {
            return 1;
        }else{
            return 2;
        }
    }
}
