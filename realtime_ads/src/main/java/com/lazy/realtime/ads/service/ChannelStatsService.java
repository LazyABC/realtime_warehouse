package com.lazy.realtime.ads.service;

import com.lazy.realtime.ads.bean.traffic.ChannelSvCt;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 18:17:03
 * @Details:
 */
public interface ChannelStatsService {

    //计算各渠道独立访客数
    List<ChannelSvCt> queryTrafficUvCtByChannel(String date);
}
