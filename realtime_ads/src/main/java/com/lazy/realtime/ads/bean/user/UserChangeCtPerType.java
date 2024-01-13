package com.lazy.realtime.ads.bean.user;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 15:17:55
 * @Details: 用户变动统计
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserChangeCtPerType {
    
    //变动类型
    String name;
    //用户数
    Integer num;
       
}
