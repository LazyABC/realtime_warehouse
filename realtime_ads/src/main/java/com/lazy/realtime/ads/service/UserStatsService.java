package com.lazy.realtime.ads.service;

import com.lazy.realtime.ads.bean.user.UserChangeCtPerType;
import com.lazy.realtime.ads.bean.user.UserPageCt;
import com.lazy.realtime.ads.bean.user.UserTradeCt;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 16:22:47
 * @Details:
 */
public interface UserStatsService {

    List<UserChangeCtPerType> getUserChangeCt(String date);

    List<UserPageCt> getUvByPage(String date);

    List<UserTradeCt> getTradeUserCt(String date);
}
