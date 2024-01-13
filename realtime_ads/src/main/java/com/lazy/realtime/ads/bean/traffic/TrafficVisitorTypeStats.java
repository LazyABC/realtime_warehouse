package com.lazy.realtime.ads.bean.traffic;

import com.lazy.realtime.ads.util.DataUtil;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 09:47:52
 * @Details: 新老访客流量统计
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficVisitorTypeStats {
    // 新老访客状态标记
    String isNew;
    // 独立访客数
    Long uvCt;
    // 页面浏览数
    Long pvCt;
    // 会话数
    Long svCt;;
    // 累计访问时长
    Long durSum;

    // 会话平均在线时长（秒）
    public Double getAvgDurSum() {
        if(svCt == 0) {
            return 0.0;
        }
        return DataUtil.validDouble((double)durSum/(double) svCt / 1000);
    }

    // 会话平均访问页面数
    public Double getAvgPvCt(){
        if(svCt == 0) {
            return 0.0;
        }
        return DataUtil.validDouble((double)pvCt / (double) svCt);
    }
}
