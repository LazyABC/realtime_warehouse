package com.lazy.realtime.ads.bean.trade;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:40:44
 * @Details:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class TradeProvinceOrderAmount {
    // 省份名称
    String name;
    // 下单金额
    Double value;
}
