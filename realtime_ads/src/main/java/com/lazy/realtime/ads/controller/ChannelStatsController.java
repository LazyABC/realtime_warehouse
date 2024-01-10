package com.lazy.realtime.ads.controller;

import com.lazy.realtime.ads.bean.channel.TrafficUvCt;
import com.lazy.realtime.ads.bean.format.SeriesBean;
import com.lazy.realtime.ads.bean.response.BarResponseData;
import com.lazy.realtime.ads.service.ChannelStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 18:11:37
 * @Details:
 * web应用开发的流程:
 *         M-V(BI程序)-C
 *             M： Bean 数据模型
 *                 Service 业务模型
 *             C：  控制器，负责接收请求，给出响应。
 *
 *     写接口，都是RestController
 *
 *     ----------------------------------
 *         返回的数据的格式，必须和人家要求的格式一致！
 *      {}:  封装Bean，或JSONObject,Map。
 *             Bean的属性名 或 Map的key，必须和要求的属性名一致！
 *      []:  封装List，或JSONArray
 */

@RestController
@RequestMapping("/gmall/realtime/traffic")
public class ChannelStatsController {

    @Autowired
    private ChannelStatsService channelStatsService;
    @RequestMapping("/uvCt")
    public Object calChannelUvct(String date){
        //既包含渠道，也有对应的指标
        List<TrafficUvCt> trafficUvCts = channelStatsService.queryTrafficUvCtByChannel(date);
        List<String> categories = trafficUvCts.stream().map(TrafficUvCt::getCh).collect(Collectors.toList());
        List<Long> seriesDataList = trafficUvCts.stream().map(TrafficUvCt::getUvCt).collect(Collectors.toList());

        return new BarResponseData(0,"",categories, Collections.singletonList(new SeriesBean("独立访客数",seriesDataList)));

    }
}
