package com.ab.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-05 12:18
 **/
@ToString
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Access {
    private String device;
    private String deviceType;
    private String os;
    private String event;
    private String net;
    private String channel;
    private String uid;
    private int nu; //1：新用户
    private String ip;
    private long time;
    private String version;

    private Product product;
}
