package com.lazy.realtime.ads.bean.traffic;

import lombok.AllArgsConstructor;
import lombok.Data;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 18:13:59
 * @Details:
 */

@Data
@AllArgsConstructor
public class ChannelSvCt {
    //渠道
    String ch;
    //会话数
    Long svCt;

}
