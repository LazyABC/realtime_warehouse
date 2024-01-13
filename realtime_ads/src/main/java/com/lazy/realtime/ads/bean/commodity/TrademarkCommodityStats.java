package com.lazy.realtime.ads.bean.commodity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:02:00
 * @Details:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrademarkCommodityStats {
    // 品牌名称
    String trademarkName;
    // 订单金额
    Double orderAmount;
    // 退单数
    Integer refundCt;
    // 退单人数
    Integer refundUuCt;
}
