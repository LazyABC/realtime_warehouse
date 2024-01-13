package com.lazy.realtime.ads.bean.user;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 15:22:35
 * @Details: 新增交易用户
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserTradeCt {
    //交易类型
    String name;
    //用户数
    Integer num;

}