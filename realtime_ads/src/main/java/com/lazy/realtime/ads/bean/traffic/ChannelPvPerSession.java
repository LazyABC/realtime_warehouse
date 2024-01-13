package com.lazy.realtime.ads.bean.traffic;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 09:36:25
 * @Details:
 */

@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChannelPvPerSession {
    //渠道
    String ch;
    //各会话页面浏览数
    Double pvPerSession;
}
