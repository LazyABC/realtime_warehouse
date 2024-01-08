package com.lazy.realtime.dws.trade.pojo;

import com.alibaba.fastjson.annotation.JSONField;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * @Name: Lazy
 * @Date: 2024/1/8 14:43:58
 * @Details:
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class PaySucWindowBean {
    @JSONField(serialize = false)
    private String userId;
    private String stt;
    private String edt;
    private String curDate;
    //添加dws表需要的指标字段
    private Long paymentSucUniqueUserCount = 0l;
    private Long paymentSucNewUserCount = 0l;

    @JSONField(serialize = false)
    private Long ts;
}