package com.lazy.realtime.ads.service.impl;

import com.lazy.realtime.ads.bean.trade.TradeProvinceOrderAmount;
import com.lazy.realtime.ads.bean.trade.TradeProvinceOrderCt;
import com.lazy.realtime.ads.bean.trade.TradeStats;
import com.lazy.realtime.ads.mapper.TradeStatsMapper;
import com.lazy.realtime.ads.service.TradeStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 13:53:43
 * @Details:
 */
@Service
public class TradeStatsServiceImpl implements TradeStatsService {

    @Autowired
    private TradeStatsMapper mapper;

    @Override
    public Double getTotalAmount(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectTotalAmount(date);
    }

    @Override
    public List<TradeStats> getTradeStats(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectTradeStats(date);
    }

    @Override
    public List<TradeProvinceOrderCt> getTradeProvinceOrderCt(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectTradeProvinceOrderCt(date);
    }

    @Override
    public List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectTradeProvinceOrderAmount(date);
    }
}
