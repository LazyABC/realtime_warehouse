package com.lazy.realtime.ads.bean.activity;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 14:52:57
 * @Details:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ActivityReduceStats {
    // 活动减免金额
    Double activityReduceAmount;
    // 原始金额
    Double originTotalAmount;
    // 活动补贴率
    Double activitySubsidyRate;
}
