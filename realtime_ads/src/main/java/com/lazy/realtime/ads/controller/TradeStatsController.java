package com.lazy.realtime.ads.controller;

import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.ads.bean.format.TableFormatColumn;
import com.lazy.realtime.ads.bean.trade.TradeProvinceOrderAmount;
import com.lazy.realtime.ads.bean.trade.TradeProvinceOrderCt;
import com.lazy.realtime.ads.bean.trade.TradeStats;
import com.lazy.realtime.ads.service.TradeStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 13:58:38
 * @Details:
 */
@RestController
@RequestMapping("/gmall/realtime/trade")
public class TradeStatsController
{
    @Autowired
    private TradeStatsService service;
    //5.1 交易综合统计  翻牌器
    @RequestMapping("/total")
    public Object getTradeTotal(String date) {

        Double amount = DataUtil.validDouble(service.getTotalAmount(date));

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",amount);
        return result;

    }

    //5.1 交易综合统计  表格
    @RequestMapping("/stats")
    public Object getTradeTotalStats(String date) {

        List<TradeStats> list = service.getTradeStats(date);

        JSONObject data = new JSONObject();
        data.put("columns", Arrays.asList(
                new TableFormatColumn("指标","name"),
                new TableFormatColumn("数值","num")
        ));
        data.put("rows",list);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",data);
        return result;


    }

    //5.2 各省份订单数统计  地图
    @RequestMapping("/provinceOrderCt")
    public Object getProvinceOrderStats(String date) {

        List<TradeProvinceOrderCt> mapData = service.getTradeProvinceOrderCt(date);
        JSONObject data = new JSONObject();
        data.put("valueName","订单数");
        data.put("mapData",mapData);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",data);
        return result;

    }

    //5.2 各省份订单金额统计  地图
    @RequestMapping("/provinceOrderAmount")
    public Object getProvinceOrderAmountStats(String date) {

        List<TradeProvinceOrderAmount> mapData = service.getTradeProvinceOrderAmount(date);
        JSONObject data = new JSONObject();
        data.put("valueName","订单金额");
        data.put("mapData",mapData);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",data);
        return result;

    }
}
