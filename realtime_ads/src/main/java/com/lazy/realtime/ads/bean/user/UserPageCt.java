package com.lazy.realtime.ads.bean.user;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 15:20:54
 * @Details: 漏斗分析
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UserPageCt {
    //页面
    String name;
    //独立访客数
    Integer value;
}
