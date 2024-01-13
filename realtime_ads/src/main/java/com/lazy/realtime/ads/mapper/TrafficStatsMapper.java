package com.lazy.realtime.ads.mapper;

import com.lazy.realtime.ads.bean.traffic.*;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/12 09:52:45
 * @Details: 用于查询与流量统计相关的数据，包括用户访问量（UV）、会话数（SV）、页面浏览数（PV）等
 */

@Mapper
public interface TrafficStatsMapper {

    //1. 查询各渠道的UV（用户访问量）总数，按UV降序排列
    @Select("  SELECT ch," +
            "       SUM(uv_ct) uv_ct" +
            "  FROM dws_traffic_vc_ch_ar_is_new_page_view_window" +
            "           PARTITION (par${dt})" +
            "  where ch is not null" +
            "  GROUP BY ch" +
            "  ORDER BY uv_ct DESC;")
    List<ChannelUvCt> queryTrafficUvCtByChannel(@Param("dt") String date);

    // 2. 获取各渠道会话数
    @Select(" select ch," +
            "       sum(sv_ct) sv_ct" +
            " from dws_traffic_vc_ch_ar_is_new_page_view_window" +
            "         partition (par${date})" +
            " group by ch" +
            " order by sv_ct desc;")
    List<ChannelSvCt> selectSvCt(@Param("date") String date);

    // 3. 获取各渠道会话平均页面浏览数
    @Select(" select ch," +
            "       sum(pv_ct) / sum(sv_ct) pv_per_session" +
            " from dws_traffic_vc_ch_ar_is_new_page_view_window" +
            "         partition (par${date})" +
            " group by ch" +
            " order by pv_per_session desc;")
    List<ChannelPvPerSession> selectPvPerSession(@Param("date") String date);

    // 4. 获取各渠道会话平均页面访问时长
    @Select(" select ch," +
            "       sum(dur_sum) / sum(sv_ct) / 1000  dur_per_session" +
            " from dws_traffic_vc_ch_ar_is_new_page_view_window" +
            "         partition (par${date})" +
            " group by ch" +
            " order by dur_per_session desc;")
    List<ChannelDurPerSession> selectDurPerSession(@Param("date") String date);


    // 2.2分时流量数据查询
    @Select
            ("select " +
            "       hour(stt)    hr," +
            "       sum(uv_ct)   uv_ct," +
            "       sum(pv_ct)   pv_ct," +
            "       sum(if(is_new = '1', dws_traffic_vc_ch_ar_is_new_page_view_window.uv_ct, 0)) new_uv_ct" +
            " from dws_traffic_vc_ch_ar_is_new_page_view_window" +
            "         partition (par${dt})" +
            " group by hr")
    List<TrafficVisitorStatsPerHour> selectVisitorStatsPerHr(@Param("dt") String date);

    @Select("select is_new," +
            "       sum(uv_ct)   uv_ct," +
            "       sum(pv_ct)   pv_ct," +
            "       sum(sv_ct)   sv_ct," +
            "       sum(dur_sum) dur_sum" +
            " from dws_traffic_vc_ch_ar_is_new_page_view_window" +
            "         partition (par${dt})" +
            " group by is_new")
    List<TrafficVisitorTypeStats> selectVisitorTypeStats(@Param("dt") String date);


    @Select(" select " +
            "       keyword name," +
            "       sum(keyword_count) `value` " +
            " from dws_traffic_source_keyword_page_view_window" +
            "         partition (par${dt})" +
            " group by keyword ")
    List<TrafficKeywords> selectKeywords(@Param("dt") String date);

}
