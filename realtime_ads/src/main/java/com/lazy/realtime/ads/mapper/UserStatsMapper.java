package com.lazy.realtime.ads.mapper;

import com.lazy.realtime.ads.bean.user.UserChangeCtPerType;
import com.lazy.realtime.ads.bean.user.UserPageCt;
import com.lazy.realtime.ads.bean.user.UserTradeCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 15:31:20
 * @Details:
 */
@Mapper
public interface UserStatsMapper {
    @Select("select '回流用户数'     name," +
            "       sum(back_ct) num " +
            "  from dws_user_user_login_window " +
            "         partition (par${dt})" +
            " union all  " +
            " select '活跃用户数'     name," +
            "        sum(uu_ct) num " +
            "    from dws_user_user_login_window " +
            "       partition (par${dt}) "  )
    List<UserChangeCtPerType> selectUserChangeCtPerType(@Param("dt") String date);

    @Select("  select 'home'          name," +
            "       sum(home_uv_ct) value" +
            "  from dws_traffic_home_detail_page_view_window" +
            "         partition (par${dt})" +
            "  union all" +
            "  select 'good_detail'          name," +
            "       sum(good_detail_uv_ct) value" +
            "  from dws_traffic_home_detail_page_view_window" +
            "         partition (par${dt})" +
            "  union all" +
            "  select 'cart'              name," +
            "       sum(cart_add_uu_ct) value" +
            "  from dws_trade_cart_add_uu_window" +
            "         partition (par${dt})" +
            "  union all" +
            "  select 'trade'                      name," +
            "       sum(order_unique_user_count) value" +
            "  from dws_trade_order_window" +
            "         partition (par${dt})" +
            "  union all" +
            "  select 'payment'                          name," +
            "       sum(payment_suc_unique_user_count) value" +
            "  from dws_trade_payment_suc_window" +
            "         partition (par${dt});")
    List<UserPageCt> selectUvByPage(@Param("dt") String date);

    @Select("  select 'order'                   name," +
            "       sum(order_new_user_count) num " +
            "  from dws_trade_order_window" +
            "         partition (par${dt})" +
            "  union all" +
            "  select 'payment'                   name," +
            "       sum(payment_suc_new_user_count) num " +
            "  from dws_trade_payment_suc_window" +
            "         partition (par${dt});")
    List<UserTradeCt> selectTradeUserCt(@Param("dt") String date);
}
