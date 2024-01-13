package com.lazy.realtime.ads.controller;

import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.ads.service.ActivityStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 15:03:38
 * @Details:
 */
@RestController
@RequestMapping("/gmall/realtime/activity")
public class ActivityStatsController {
    @Autowired
    private ActivityStatsService service;
    //7.1 用户变动统计
    @RequestMapping("/stats")
    public Object getActivityStats(String date) {

        Double rate = DataUtil.validDouble(service.getActivityStats(date) * 100);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",rate);
        return result;

    }
}
