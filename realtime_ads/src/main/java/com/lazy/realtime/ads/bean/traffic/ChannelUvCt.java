package com.lazy.realtime.ads.bean.traffic;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 09:32:05
 * @Details:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChannelUvCt {
    //渠道
    String ch;
    //独立访客数
    Long uvCt;

}
