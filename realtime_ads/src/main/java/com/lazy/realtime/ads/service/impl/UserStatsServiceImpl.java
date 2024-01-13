package com.lazy.realtime.ads.service.impl;

import com.lazy.realtime.ads.bean.user.UserChangeCtPerType;
import com.lazy.realtime.ads.bean.user.UserPageCt;
import com.lazy.realtime.ads.bean.user.UserTradeCt;
import com.lazy.realtime.ads.mapper.UserStatsMapper;
import com.lazy.realtime.ads.service.UserStatsService;
import com.lazy.realtime.ads.util.DataUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 16:26:42
 * @Details:
 */
@Service
public class UserStatsServiceImpl implements UserStatsService
{
    @Autowired
    private UserStatsMapper mapper;
    @Override
    public List<UserChangeCtPerType> getUserChangeCt(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectUserChangeCtPerType(date);
    }

    @Override
    public List<UserPageCt> getUvByPage(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectUvByPage(date);
    }

    @Override
    public List<UserTradeCt> getTradeUserCt(String date) {
        date = DataUtil.validDate(date);
        return mapper.selectTradeUserCt(date);
    }
}