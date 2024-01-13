package com.lazy.realtime.ads.mapper;

import com.lazy.realtime.ads.bean.commodity.CategoryCommodityStats;
import com.lazy.realtime.ads.bean.commodity.SpuCommodityStats;
import com.lazy.realtime.ads.bean.commodity.TrademarkCommodityStats;
import com.lazy.realtime.ads.bean.commodity.TrademarkOrderAmountPieGraph;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/13 11:04:14
 * @Details:
 */
@Mapper
public interface CommodityStatsMapper {
    @Select("select oct.trademark_name trademarkName," +
            "       order_amount orderAmount," +
            "       refundCt," +
            "       refundUuCt" +
            "  from (  " +
            "           select trademark_id," +
            "             trademark_name," +
            "             sum(order_amount) order_amount" +
            "             from dws_trade_sku_order_window" +
            "                 partition (par${dt})" +
            "             group by trademark_id, trademark_name" +
            "         ) oct " +
            "          full  join" +
            "          (" +
            "                   select trademark_id," +
            "                         trademark_name," +
            "                         sum(refund_count)       refundCt," +
            "                         count(distinct user_id) refundUuCt" +
            "                  from dws_trade_trademark_category_user_refund_window" +
            "                  partition (par${dt})" +
            "                  group by trademark_id, trademark_name" +
            "              ) rct" +
            "       on oct.trademark_id = rct.trademark_id;")
    List<TrademarkCommodityStats> selectTrademarkStats(@Param("dt") String date);

    @Select("select trademark_name name," +
            "       sum(order_amount) `value` " +
            " from dws_trade_sku_order_window" +
            "         partition (par${dt})" +
            " group by trademark_id, trademark_name;")
    List<TrademarkOrderAmountPieGraph> selectTmOrderAmtPieGra(@Param("dt") String date);

    @Select(" select oct.category1_name," +
            "       oct.category2_name," +
            "       oct.category3_name," +
            "       order_amount," +
            "       refund_count," +
            "       refund_uu_count" +
            "  from (select category1_id," +
            "             category1_name," +
            "             category2_id," +
            "             category2_name," +
            "             category3_id," +
            "             category3_name," +
            "             sum(order_amount) order_amount" +
            "      from dws_trade_sku_order_window" +
            "               partition (par${dt})" +
            "      group by category1_id," +
            "               category1_name," +
            "               category2_id," +
            "               category2_name," +
            "               category3_id," +
            "               category3_name) oct full outer join" +
            "     (select category1_id," +
            "             category1_name," +
            "             category2_id," +
            "             category2_name," +
            "             category3_id," +
            "             category3_name," +
            "             sum(refund_count)       refund_count," +
            "             count(distinct user_id) refund_uu_count" +
            "      from dws_trade_trademark_category_user_refund_window" +
            "    partition (par${dt})" +
            "      group by category1_id," +
            "               category1_name," +
            "               category2_id," +
            "               category2_name," +
            "               category3_id," +
            "               category3_name) rct" +
            "  on oct.category1_id = rct.category1_id" +
            "    and oct.category2_id = rct.category2_id" +
            "    and oct.category3_id = rct.category3_id;")
    List<CategoryCommodityStats> selectCategoryStats(@Param("dt") String date);

    @Select(" select spu_name," +
            "       sum(order_amount) order_amount" +
            " from dws_trade_sku_order_window" +
            "         partition (par${dt})" +
            " group by spu_id, spu_name;")
    List<SpuCommodityStats> selectSpuStats(@Param("dt") String date);
}
