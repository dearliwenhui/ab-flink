package com.ab.flink.domain;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;

/**
 * @description:
 * @version: 0.0.1
 * @author: liwenhui
 * @createTime: 2021-09-05 12:20
 **/
@ToString
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Product {
    public String category;
    public String name;
}
