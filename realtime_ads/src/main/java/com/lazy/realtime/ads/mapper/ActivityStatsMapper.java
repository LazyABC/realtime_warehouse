package com.lazy.realtime.ads.mapper;


import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 14:55:22
 * @Details:
 */
@Mapper
public interface ActivityStatsMapper {

    @Select("select" +
            "   sum(activity_reduce_amount) / sum(original_amount)   rate" +
            " from dws_trade_sku_order_window" +
            "   partition (par${dt})")
    Double selectActivityStats(@Param("dt") String date);
}
