package com.lazy.realtime.ads.bean.response;

import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.ads.bean.format.SeriesBean;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 19:03:49
 * @Details:
 */
@Data
@NoArgsConstructor
public class BarResponseData {


    private Integer status;
    private String msg;
    private JSONObject data;

    public BarResponseData(Integer status, String msg, List<String> categories, List<SeriesBean> series){
        this.status = status;
        this.msg = msg;
        data = new JSONObject();
        data.put("categories", categories);
        data.put("series", series);
    }
}
