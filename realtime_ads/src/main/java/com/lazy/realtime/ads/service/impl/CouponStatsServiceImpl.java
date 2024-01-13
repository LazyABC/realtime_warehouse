package com.lazy.realtime.ads.service.impl;

import com.lazy.realtime.ads.mapper.CommodityStatsMapper;
import com.lazy.realtime.ads.mapper.CouponStatsMapper;
import com.lazy.realtime.ads.service.CouponStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 14:38:33
 * @Details:
 */
@Service
public class CouponStatsServiceImpl implements CouponStatsService {

    @Autowired
    private CouponStatsMapper mapper;
    @Override
    public Double getCouponStats(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectCouponStats(date);
    }
}
