package com.lazy.realtime.ads.bean.coupon;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 14:27:25
 * @Details:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class CouponReduceStats {
    // 优惠券减免金额
    Double couponReduceAmount;
    // 原始金额
    Double originTotalAmount;
    // 优惠券补贴率
    Double couponSubsidyRate;

}
