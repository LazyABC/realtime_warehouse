package com.lazy.realtime.ads.service;

import com.lazy.realtime.ads.bean.commodity.CategoryCommodityStats;
import com.lazy.realtime.ads.bean.commodity.SpuCommodityStats;
import com.lazy.realtime.ads.bean.commodity.TrademarkCommodityStats;
import com.lazy.realtime.ads.bean.commodity.TrademarkOrderAmountPieGraph;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:06:37
 * @Details:
 */
public interface CommodityStatsService {
    List<TrademarkCommodityStats> getTrademarkCommodityStatsService(String date);

    List<TrademarkOrderAmountPieGraph> getTmOrderAmtPieGra(String date);

    List<CategoryCommodityStats> getCategoryStatsService(String date);

    List<SpuCommodityStats> getSpuCommodityStats(String date);

}
