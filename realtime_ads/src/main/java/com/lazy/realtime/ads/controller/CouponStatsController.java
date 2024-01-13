package com.lazy.realtime.ads.controller;

import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.ads.service.CouponStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.xml.crypto.Data;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 14:43:33
 * @Details:
 */
@RestController
@RequestMapping("/gmall/realtime/coupon")
public class CouponStatsController {
    @Autowired
    private CouponStatsService service;

    //用户变动统计
    @RequestMapping("/stats")
    public Object getCouponStats(String date){
        Double rate = DataUtil.validDouble(service.getCouponStats(date) * 100);
        JSONObject result = new JSONObject();
        result.put("status", 0);
        result.put("msg", "");
        result.put("data", rate);
        return result;

    }

}
