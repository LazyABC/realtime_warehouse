package com.lazy.realtime.ads.bean.channel;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 18:13:59
 * @Details:
 */

@Data
@AllArgsConstructor
public class TrafficUvCt {
    //渠道
    String ch;
    //独立访客数
    Long uvCt;

}
