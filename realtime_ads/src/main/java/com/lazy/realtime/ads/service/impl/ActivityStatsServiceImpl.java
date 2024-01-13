package com.lazy.realtime.ads.service.impl;

import com.lazy.realtime.ads.mapper.ActivityStatsMapper;
import com.lazy.realtime.ads.service.ActivityStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 15:01:42
 * @Details:
 */
@Service
public class ActivityStatsServiceImpl implements ActivityStatsService {
    @Autowired
    private ActivityStatsMapper mapper;

    @Override
    public Double getActivityStats(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectActivityStats(date);
    }
}
