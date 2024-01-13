package com.lazy.realtime.ads.service.impl;

import com.lazy.realtime.ads.bean.traffic.*;
import com.lazy.realtime.ads.mapper.TrafficStatsMapper;
import com.lazy.realtime.ads.service.TrafficStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import javax.xml.crypto.Data;
import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 10:16:53
 * @Details: 该类实现了TrafficStatsService接口中定义的方法，主要负责处理网站流量统计的业务逻辑。
 */
@Service
public class TrafficStatsServiceImpl implements TrafficStatsService {
    //渠道相关统计
    @Autowired
    private TrafficStatsMapper trafficStatsMapper;


    @Override
    public List<ChannelUvCt> queryTrafficUvCtByChannel(String date) {
        date = DataUtil.validDate(date);
        return trafficStatsMapper.queryTrafficUvCtByChannel(date);


    }

    //获取各渠道会话数
    @Override
    public List<ChannelSvCt> getSvCt(String date) {
        date = DataUtil.validDate(date);
        return trafficStatsMapper.selectSvCt(date);
    }

    //获取各渠道会话平均页面浏览数
    @Override
    public List<ChannelPvPerSession> getPvPerSession(String date) {
        date = DataUtil.validDate(date);
        return trafficStatsMapper.selectPvPerSession(date);
    }

    //获取各渠道会平均页面浏览数
    @Override
    public List<ChannelDurPerSession> getDurPerSession(String date) {
        date = DataUtil.validDate(date);
        return trafficStatsMapper.selectDurPerSession(date);
    }

    //流量分时统计
    @Override
    public List<TrafficVisitorStatsPerHour> getVisitorPerHrStats(String date) {
        date = DataUtil.validDate(date);
        return trafficStatsMapper.selectVisitorStatsPerHr(date);
    }

    //
    @Override
    public List<TrafficVisitorTypeStats> getVisitorTypeStats(String date) {
        date = DataUtil.validDate(date);
        return trafficStatsMapper.selectVisitorTypeStats(date);
    }

    @Override
    public List<TrafficKeywords> getKeywords(String date) {
        date = DataUtil.validDate(date);
        return trafficStatsMapper.selectKeywords(date);
    }
}
