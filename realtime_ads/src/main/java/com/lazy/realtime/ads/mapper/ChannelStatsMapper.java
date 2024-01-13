package com.lazy.realtime.ads.mapper;

import com.lazy.realtime.ads.bean.traffic.ChannelSvCt;
import org.apache.ibatis.annotations.Mapper;
import org.apache.ibatis.annotations.Param;
import org.apache.ibatis.annotations.Select;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 18:23:22
 * @Details: 执行一个 SQL 查询，统计指定日期的每个渠道的总访问量，并按访问量降序排序。
 *           返回的结果是一个包含 TrafficUvCt 对象的列表。请确保数据库中存在相应的表和字段，以及与 TrafficUvCt 类型匹配的结果
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
    List<ChannelSvCt> queryTrafficUvCtByChannel(@Param("dt") String date);

}
