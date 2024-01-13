package com.lazy.realtime.ads.controller;

import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.ads.bean.format.SeriesBean;
import com.lazy.realtime.ads.bean.format.TableFormatColumn;
import com.lazy.realtime.ads.bean.response.BarResponseData;
import com.lazy.realtime.ads.bean.traffic.*;
import com.lazy.realtime.ads.service.TrafficStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 11:38:16
 * @Details:
 */
@RestController
@RequestMapping("/gmall/realtime/traffic")
//@ConditionalOnProperty(name = "myapp.feature.enabled", havingValue = "false")
public class TrafficStatsController {
    @Autowired
    private TrafficStatsService trafficStatsService;

    @RequestMapping("/uvCt")
    public Object calChannelUvct(String date){
        //既包含渠道，也有对应指标
        List<ChannelUvCt> channelUvCts = trafficStatsService.queryTrafficUvCtByChannel(date);
        List<String> categories = channelUvCts.stream().map(ChannelUvCt::getCh).collect(Collectors.toList());
        List<Long> seriesDataList = channelUvCts.stream().map(ChannelUvCt::getUvCt).collect(Collectors.toList());

        return new BarResponseData(0,"",categories, Collections.singletonList(new SeriesBean("独立访客数",seriesDataList)));

    }

    //2.当日各渠道会话总数
    @RequestMapping("/svCt")
    public Object getPvCt(String date) {

        List<ChannelSvCt> channelSvCtList = trafficStatsService.getSvCt(date);

        List<String> categories = channelSvCtList.stream().map(ChannelSvCt::getCh).collect(Collectors.toList());
        List<Long> data = channelSvCtList.stream().map(ChannelSvCt::getSvCt).collect(Collectors.toList());
        SeriesBean<Long> series = new SeriesBean<>("会话数", data);

        return  new BarResponseData(0, "", categories, Collections.singletonList(series));

    }

    // 3. 当日各渠道会话平均浏览页面数
    @RequestMapping("/pvPerSession")
    public Object getPvPerSession(String date) {

        List<ChannelPvPerSession> channelPvPerSessionList = trafficStatsService.getPvPerSession(date);
        List<String> categories = channelPvPerSessionList.stream().map(ChannelPvPerSession::getCh).collect(Collectors.toList());
        List<Double> data = channelPvPerSessionList.stream().map(ChannelPvPerSession::getPvPerSession).map(DataUtil::validDouble).collect(Collectors.toList());
        SeriesBean<Double> series = new SeriesBean<>("会话平均浏览页面数", data);

        return  new BarResponseData( 0,"", categories, Collections.singletonList(series));

    }

    // 4. 当日各渠道会话会话平均停留时长
    @RequestMapping("/durPerSession")
    public Object getDurPerSession(String date) {

        List<ChannelDurPerSession> channelDurPerSessionList = trafficStatsService.getDurPerSession(date);

        List<String> categories = channelDurPerSessionList.stream().map(ChannelDurPerSession::getCh).collect(Collectors.toList());
        List<Double> data = channelDurPerSessionList.stream().map(ChannelDurPerSession::getDurPerSession).map(DataUtil::validDouble).collect(Collectors.toList());
        SeriesBean<Double> series = new SeriesBean<>("会话平均停留时长", data);

        return  new BarResponseData(0,"", categories, Collections.singletonList(series));

    }



    //2.2流量分时统计
    @RequestMapping("/visitorPerHr")
    public Object getVisitorPerHour(String date){
        List<TrafficVisitorStatsPerHour> visitorPerHrStatsList = trafficStatsService.getVisitorPerHrStats(date);

        List<String> hourList = new ArrayList<>();
        List<Long> uvList = new ArrayList<>();
        List<Long> pvList = new ArrayList<>();
        List<Long> newUvList = new ArrayList<>();

        for (TrafficVisitorStatsPerHour bean : visitorPerHrStatsList){
            hourList.add(bean.getHr());
            uvList.add(bean.getUvCt());
            pvList.add(bean.getPvCt());
            newUvList.add(bean.getNewUvCt());
        }

        SeriesBean<Long> series1 = new SeriesBean<>("独立访客数", uvList);
        SeriesBean<Long> series2 = new SeriesBean<>("页面浏览数", pvList);
        SeriesBean<Long> series3 = new SeriesBean<>("新访客数", newUvList);

        return new BarResponseData(0, "", hourList, Arrays.asList(series1,series2,series3));
    }


    //2.3 新老访客流量统计
    @RequestMapping("/visitorPerType")
    public Object getVisitorPerType(String date) {

        List<TrafficVisitorTypeStats> visitorTypeStatsList = trafficStatsService.getVisitorTypeStats(date);

        JSONObject data = new JSONObject();
        data.put("total",5);
        data.put("columns",Arrays.asList(
                new TableFormatColumn("类别","type"),
                new TableFormatColumn("新访客","new"),
                new TableFormatColumn("老访客","old")
        ));

        JSONObject row1 = new JSONObject();
        row1.put("type","访客数(人)");
        JSONObject row2 = new JSONObject();
        row2.put("type","总访问页面数(次)");
        JSONObject row3 = new JSONObject();
        row3.put("type","平均在线时长(秒)");
        JSONObject row4 = new JSONObject();
        row4.put("type","平均访问页面数(人次)");

        for (TrafficVisitorTypeStats bean : visitorTypeStatsList) {
            if ("0".equals(bean.getIsNew())){
                row1.put("old",bean.getUvCt());
                row2.put("old",bean.getPvCt());
                row3.put("old",bean.getAvgDurSum());
                row4.put("old",bean.getAvgPvCt());
            }else {
                row1.put("new",bean.getUvCt());
                row2.put("new",bean.getPvCt());
                row3.put("new",bean.getAvgDurSum());
                row4.put("new",bean.getAvgPvCt());
            }
        }

        data.put("rows",Arrays.asList(row1,row2,row3,row4));


        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("data",data);
        return result;

    }

    //2.4关键词统计
    @RequestMapping("/keywords")
    public Object getKeywords(String date){

        List<TrafficKeywords> data = trafficStatsService.getKeywords(date);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",data);
        return result;
    }


}
