package com.lazy.realtime.ads.bean.commodity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:03:17
 * @Details:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class SpuCommodityStats {
    // SPU 名称
    String spuName;
    // 下单金额
    Double orderAmount;
}
