package com.lazy.realtime.ads.controller;


import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.ads.bean.format.TableFormatColumn;
import com.lazy.realtime.ads.bean.user.UserChangeCtPerType;
import com.lazy.realtime.ads.bean.user.UserPageCt;
import com.lazy.realtime.ads.bean.user.UserTradeCt;
import com.lazy.realtime.ads.service.UserStatsService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Arrays;
import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 16:32:33
 * @Details:
 */

@RestController
@RequestMapping("/gmall/realtime/user/")
//@ConditionalOnProperty(name = "myapp.feature.enabled", havingValue = "true")
public class UserStatsController {
    @Autowired
    private UserStatsService userStatsService;
    //3.1 用户变动统计
    @RequestMapping("/userChangeCt")
    public Object getkeywordsStats(String date) {

        List<UserChangeCtPerType> list = userStatsService.getUserChangeCt(date);

        JSONObject data = new JSONObject();
        data.put("columns", Arrays.asList(
                new TableFormatColumn("变动类型","name"),
                new TableFormatColumn("用户数","num")
        ));
        data.put("rows",list);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",data);
        return result;

    }

    //3.2 用户行为漏斗分析
    @RequestMapping("/uvPerPage")
    public Object getFunnelStats(String date) {

        List<UserPageCt> list = userStatsService.getUvByPage(date);


        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",list);
        return result;

    }

    //3.3 新增交易用户分析
    @RequestMapping("/userTradeCt")
    public Object getNewStats(String date) {

        List<UserTradeCt> list = userStatsService.getTradeUserCt(date);

        JSONObject data = new JSONObject();
        data.put("columns", Arrays.asList(
                new TableFormatColumn("交易类型","name"),
                new TableFormatColumn("新增用户数","num")
        ));
        data.put("rows",list);

        JSONObject result = new JSONObject();
        result.put("status",0);
        result.put("msg","");
        result.put("data",data);
        return result;
    }
}
