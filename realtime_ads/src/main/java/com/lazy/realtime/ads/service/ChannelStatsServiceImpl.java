package com.lazy.realtime.ads.service;

import com.lazy.realtime.ads.bean.channel.TrafficUvCt;
import com.lazy.realtime.ads.mapper.ChannelStatsMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 18:21:23
 * @Details:
 */
@Service
public class ChannelStatsServiceImpl implements ChannelStatsService{

    //用于验证传入的日期字符串是否合法。
    // 如果传入的日期字符串为空（!StringUtils.hasText(date)），则使用当前日期的格式化形式（"yyyyMMdd"）作为默认日期
    private void validDate(String date){
        if (!StringUtils.hasText(date)){
            date = LocalDate.now().format(DateTimeFormatter.ofPattern("yyyyMMdd"));
        }
    }


    @Autowired
    private ChannelStatsMapper channelStatsMapper;


    @Override
    public List<TrafficUvCt> queryTrafficUvCtByChannel(String date) {
            validDate(date);
        return channelStatsMapper.queryTrafficUvCtByChannel(date);
    }
}
