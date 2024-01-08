package com.lazy.realtime.dws.traffic.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/7 19:13:04
 * @Details: fastjson框架，自动帮你处理驼峰和下划线才命名映射。
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class TrafficPageView {
    //要统计的指标所需要的字段
    private String mid;
    private Long duringTime;
    private String sid;
    //分组的维度
    private String vc;
    private String ar;
    private String ch;
    private String isNew;
    //提供dws表需要的其他字段
    //在做json解析时，只会把json中同名的字段的值赋值给你pojo
    private String stt;
    private String edt;
    private String curDate;
    //添加dws表需要的指标字段
    private Long uvCt = 0l;
    private Long svCt = 0l;
    private Long pvCt = 1l;
    private Long durSum = 0l;
    //添加时间 毫秒
    private long ts;
}
