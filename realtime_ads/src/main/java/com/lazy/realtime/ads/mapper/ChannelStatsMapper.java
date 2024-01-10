package com.lazy.realtime.ads.mapper;

import com.lazy.realtime.ads.bean.channel.TrafficUvCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 18:23:22
 * @Details:
 */
@Mapper
public interface ChannelStatsMapper {

    @Select("  SELECT ch," +
            "       SUM(uv_ct) uv_ct" +
            "  FROM dws_traffic_vc_ch_ar_is_new_page_view_window" +
            "           PARTITION (par${dt})" +
            "  where ch is not null" +
            "  GROUP BY ch" +
            "  ORDER BY uv_ct DESC;")
    List<TrafficUvCt> queryTrafficUvCtByChannel(@Param("dt") String date);

}
