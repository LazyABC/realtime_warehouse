package com.lazy.realtime.ads.service.impl;

import com.lazy.realtime.ads.bean.commodity.CategoryCommodityStats;
import com.lazy.realtime.ads.bean.commodity.SpuCommodityStats;
import com.lazy.realtime.ads.bean.commodity.TrademarkCommodityStats;
import com.lazy.realtime.ads.bean.commodity.TrademarkOrderAmountPieGraph;
import com.lazy.realtime.ads.mapper.CommodityStatsMapper;
import com.lazy.realtime.ads.service.CommodityStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:11:27
 * @Details:
 */
@Service
public class CommodityStatsServiceImpl implements CommodityStatsService {
    @Autowired
    private CommodityStatsMapper mapper;


    @Override
    public List<TrademarkCommodityStats> getTrademarkCommodityStatsService(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectTrademarkStats(date);
    }

    @Override
    public List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectTmOrderAmtPieGra(date);
    }

    @Override
    public List<CategoryCommodityStats> getCategoryStatsService(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectCategoryStats(date);
    }

    @Override
    public List<SpuCommodityStats> getSpuCommodityStats(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectSpuStats(date);
    }
}
