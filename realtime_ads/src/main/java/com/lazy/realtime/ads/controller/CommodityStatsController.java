package com.lazy.realtime.ads.controller;

import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.ads.bean.commodity.CategoryCommodityStats;
import com.lazy.realtime.ads.bean.commodity.SpuCommodityStats;
import com.lazy.realtime.ads.bean.commodity.TrademarkCommodityStats;
import com.lazy.realtime.ads.bean.commodity.TrademarkOrderAmountPieGraph;
import com.lazy.realtime.ads.bean.format.TableFormatColumn;
import com.lazy.realtime.ads.service.CommodityStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:20:10
 * @Details:
 */
@RestController
@RequestMapping("/gmall/realtime/commodity")
public class CommodityStatsController {

    @Autowired
    private CommodityStatsService service;
    //各品牌商品交易统计 轮播表格
    @RequestMapping("/trademark")
    public Object getTmTradeStats(String date) {

        List<TrademarkCommodityStats> list = service.getTrademarkCommodityStatsService(date);

        JSONObject data = new JSONObject();
        data.put("columns", Arrays.asList(
                new TableFormatColumn("品牌名称","trademarkName"),
                new TableFormatColumn("订单金额","orderAmount"),
                new TableFormatColumn("退单数","refundCt"),
                new TableFormatColumn("退单人数","refundUuCt")
        ));
        data.put("rows",list);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",data);
        return result;

    }

    //各品牌商品交易统计  饼图
    @RequestMapping("/tmPieGraph")
    public Object getTmTradePieStats(String date) {

        List<TrademarkOrderAmountPieGraph> list = service.getTmOrderAmtPieGra(date);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",list);
        return result;

    }

    //各品类商品交易统计 轮播表格
    @RequestMapping("/category")
    public Object getCategoryTradePieStats(String date) {

        List<CategoryCommodityStats> list = service.getCategoryStatsService(date);

        JSONObject data = new JSONObject();
        data.put("columns", Arrays.asList(
                new TableFormatColumn("一级品类名称","category1Name"),
                new TableFormatColumn("二级品类名称","category2Name"),
                new TableFormatColumn("三级品类名称","category3Name"),
                new TableFormatColumn("订单金额","orderAmount"),
                new TableFormatColumn("退单数","refundCount"),
                new TableFormatColumn("退单人数","refundUuCount")
        ));
        data.put("rows",list);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",data);
        return result;

    }

    //各spu商品交易统计  轮播表格
    @RequestMapping("/spu")
    public Object getSpuTradePieStats(String date) {

        List<SpuCommodityStats> list = service.getSpuCommodityStats(date);

        JSONObject data = new JSONObject();
        data.put("columns", Arrays.asList(
                new TableFormatColumn("SPU 名称","spuName"),
                new TableFormatColumn("订单金额","orderAmount")
        ));
        data.put("rows",list);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",data);
        return result;

    }
}
