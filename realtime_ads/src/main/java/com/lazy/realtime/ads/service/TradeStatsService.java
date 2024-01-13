package com.lazy.realtime.ads.service;

import com.lazy.realtime.ads.bean.trade.TradeProvinceOrderAmount;
import com.lazy.realtime.ads.bean.trade.TradeProvinceOrderCt;
import com.lazy.realtime.ads.bean.trade.TradeStats;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 13:52:41
 * @Details:
 */
public interface TradeStatsService {
    Double getTotalAmount(String date);

    List<TradeStats> getTradeStats(String date);

    List<TradeProvinceOrderCt> getTradeProvinceOrderCt(String date);

    List<TradeProvinceOrderAmount> getTradeProvinceOrderAmount(String date);
}
