package com.lazy.realtime.ads.bean.traffic;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 09:46:37
 * @Details: 流量分时统计
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficVisitorStatsPerHour {
    // 小时
    String hr;
    // 独立访客数
    Long uvCt;
    // 页面浏览数
    Long pvCt;
    // 新访客数
    Long newUvCt;
}
