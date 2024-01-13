package com.lazy.realtime.ads.bean.commodity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:02:44
 * @Details:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CategoryCommodityStats {
    // 一级品类名称
    String category1Name;
    // 二级品类名称
    String category2Name;
    // 三级品类名称
    String category3Name;
    // 订单金额
    Double orderAmount;
    // 退单数
    Integer refundCount;
    // 退单人数
    Integer refundUuCount;
}
