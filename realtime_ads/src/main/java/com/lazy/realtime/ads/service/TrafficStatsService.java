package com.lazy.realtime.ads.service;

import com.lazy.realtime.ads.bean.traffic.*;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 10:08:56
 * @Details: 用于调用数据库查询的业务逻辑层
 */
public interface TrafficStatsService {
    //2.1 计算各渠道独立访客数
    List<ChannelUvCt> queryTrafficUvCtByChannel(String date);

    // 2.1 获取各渠道会话数
    List<ChannelSvCt> getSvCt(String date);

    // 2.1 获取各渠道会话平均页面浏览数
    List<ChannelPvPerSession> getPvPerSession(String date);

    // 2.1 获取各渠道会话平均页面访问时长
    List<ChannelDurPerSession> getDurPerSession(String date);

    // 2.2 获取分时流量数据
    List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(String date);

    //2.3新老客户流量统计
    List<TrafficVisitorTypeStats> getVisitorTypeStats(String date);

    //2.4 关键词统计
    List<TrafficKeywords> getKeywords(String date);

}
