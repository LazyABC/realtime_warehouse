package com.lazy.realtime.dim.pojo;

import lombok.Data;

@Data
public class TableProcess {
    // 来源表
    String sourceTable;
    // 来源操作类型
    String sourceType;
    // 输出表
    String sinkTable;
    // 输出类型 dwd | dim
    String sinkType;
    // 数据到 hbase 的列族
    String sinkFamily;
    // 输出字段
    String sinkColumns;
    // sink到 hbase 的时候的主键字段
    String sinkRowKey;
    // 额外添加
    String op; // 配置表操作: c r u d
}