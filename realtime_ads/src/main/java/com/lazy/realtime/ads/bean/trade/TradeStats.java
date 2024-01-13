package com.lazy.realtime.ads.bean.trade;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:39:50
 * @Details:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeStats {
    // 指标类型
    String name;
    // 度量值
    Integer num;
}
