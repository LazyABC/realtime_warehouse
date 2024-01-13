package com.lazy.realtime.ads.mapper;

import com.lazy.realtime.ads.bean.trade.TradeProvinceOrderAmount;
import com.lazy.realtime.ads.bean.trade.TradeProvinceOrderCt;
import com.lazy.realtime.ads.bean.trade.TradeStats;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:45:33
 * @Details:
 */
@Mapper
public interface TradeStatsMapper {
    // 5.1.1 轮播图
    @Select(" select " +
            " sum(order_amount) order_total_amount " +
            " from dws_trade_province_order_window" +
            "         partition (par${dt});")
    Double selectTotalAmount(@Param("dt")String dt);

    // 5.1.2 轮播表格
    @Select(" select " +
            "      '订单数' name," +
            "       sum(order_count) num" +
            " from dws_trade_province_order_window" +
            " partition (par${dt})" +
            " union all" +
            " select " +
            "      '下单人数' name," +
            "       sum(order_unique_user_count) num" +
            "  from dws_trade_order_window" +
            "  partition (par${dt})" +
            "  union all" +
            "   select " +
            "      '退单数' name," +
            "       sum(refund_count)       num" +
            "  from dws_trade_trademark_category_user_refund_window" +
            "  partition (par${dt})" +
            "  union all" +
            "  select " +
            "      '退单人数'                  name," +
            "       count(distinct user_id) num" +
            " from dws_trade_trademark_category_user_refund_window" +
            "         partition (par${dt});")
    List<TradeStats> selectTradeStats(@Param("dt")String dt);

    //5.2 省份订单数
    @Select(" select province_name name, " +
            "       sum(order_count) `value` " +
            " from dws_trade_province_order_window " +
            "         partition (par${dt}) " +
            " group by province_id, province_name;")
    List<TradeProvinceOrderCt> selectTradeProvinceOrderCt(@Param("dt")String dt);

    //5.2 省份订单金额
    @Select("select province_name name," +
            "       sum(order_amount) `value` " +
            " from dws_trade_province_order_window " +
            "         partition (par${dt})" +
            " group by province_id, province_name;")
    List<TradeProvinceOrderAmount> selectTradeProvinceOrderAmount(@Param("dt")String dt);
}
