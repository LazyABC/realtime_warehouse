package com.lazy.realtime.dws.traffic.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 19:27:11
 * @Details:
 */
@Data
@NoArgsConstructor
public class HomeDetailPVBean {
    @JSONField(serialize = false)
    private String mid;
    @JSONField(serialize = false)
    private String pageId;

    private String stt;
    private String edt;
    private String curDate;
    //添加dws表需要的指标字段
    private Long home_uv_ct = 0l;
    private Long good_detail_uv_ct = 0l;
    @JSONField(serialize = false)
    private Long ts;

}
