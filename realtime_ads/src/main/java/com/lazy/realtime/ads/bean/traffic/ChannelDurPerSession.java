package com.lazy.realtime.ads.bean.traffic;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 09:41:04
 * @Details:
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class ChannelDurPerSession {
    //渠道
    String ch;
    //各会话页面访问时长
    Double durPerSession;

}
