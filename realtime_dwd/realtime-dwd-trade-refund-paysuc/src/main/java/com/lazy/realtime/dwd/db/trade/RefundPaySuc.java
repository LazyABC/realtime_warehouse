package com.lazy.realtime.dwd.db.trade;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import java.time.Duration;

import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_REFUND_PAY_SUC;

/**
 * @Name: Lazy
 * @Date: 2024/1/5 18:32:38
 * @Details:
 */
public class RefundPaySuc extends BaseSqlApp {
    public static void main(String[] args) {

        new RefundPaySuc()
                .start(
                        TOPIC_DWD_TRADE_REFUND_PAY_SUC,
                        11008,
                        4
                );
    }

    @Override
    protected void handle(TableEnvironment env) {

        //清楚从申请退单，商家同意，到商家完成退款，平台要求的极限时间。
        env.getConfig().setIdleStateRetention(Duration.ofDays(7));

        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDb(env);

        //2.获取退款成功的订单
        String refundPaySql = " select " +
                "  data['id'] id, " +
                "  data['order_id'] order_id, " +
                "  data['out_trade_no'] out_trade_no, " +
                "  data['refund_status'] refund_status, " +
                "  data['payment_type'] payment_type, " +
                "  data['trade_no'] trade_no, " +
                "  data['callback_time'] callback_time, " +
                "  ts ," +
                "  pt " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'refund_payment' " +
                " and `type` = 'update'" +
                " and `old`['refund_status'] is not null " +
                " and `data`['refund_status'] = '1602' ";

        env.createTemporaryView("rp",env.sqlQuery(refundPaySql));

        // 3.查询申请退单成功的信息   TOPIC_DWD_TRADE_ORDER_REFUND
        String refundOrderSql = "create table "+TOPIC_DWD_TRADE_ORDER_REFUND+"("+
                " id STRING, " +
                " user_id STRING, " +
                " order_id STRING, " +
                " sku_id STRING, " +
                " refund_type STRING, " +
                " refund_num STRING, " +
                " refund_amount STRING, " +
                " refund_reason_type STRING," +
                " refund_reason_txt STRING," +
                " operate_time STRING," +
                " province_id STRING ," +
                "  refund_type_name STRING  ,  " +
                "  refund_reason_type_name STRING  " + SqlUtil.getKafkaSourceSql(TOPIC_DWD_TRADE_ORDER_REFUND,"230724");

        env.executeSql(refundOrderSql);


        //4.获取维度表  dim_dic_code
        createDimBaseDic(env);

        /*
            5.关联
         */
        String joinSql = " select" +
                " rp.id, " +
                " out_trade_no, " +
                " refund_status, " +
                " payment_type, " +
                " trade_no, " +
                " callback_time, " +
                " user_id, " +
                " rp.order_id, " +
                " sku_id, " +
                " refund_type, " +
                " refund_num, " +
                " refund_amount, " +
                " refund_reason_type," +
                " refund_reason_txt," +
                " province_id ," +
                " refund_type_name  ,  " +
                " refund_reason_type_name ," +
                " dim1.info.dic_name refund_status_name ," +
                " dim2.info.dic_name payment_type_name ," +
                "  ts " +
                " from rp " +
                " join  " + TOPIC_DWD_TRADE_ORDER_REFUND +" ro " +
                " on rp.order_id = ro.order_id " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF rp.pt as dim1 " +
                "    ON rp.refund_status = dim1.id " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF rp.pt as dim2 " +
                "    ON rp.payment_type = dim2.id ";

        //6.创建sink表
        String sinkSql = "create table "+TOPIC_DWD_TRADE_REFUND_PAY_SUC+"("+
                " id STRING, " +
                " out_trade_no STRING, " +
                " refund_status STRING, " +
                " payment_type STRING, " +
                " trade_no STRING, " +
                " callback_time STRING, " +
                " user_id STRING, " +
                " order_id STRING, " +
                " sku_id STRING, " +
                " refund_type STRING, " +
                " refund_num STRING, " +
                " refund_amount STRING, " +
                " refund_reason_type STRING," +
                " refund_reason_txt STRING," +
                " province_id STRING ," +
                " refund_type_name   STRING ," +
                " refund_reason_type_name STRING ," +
                " refund_status_name STRING," +
                " payment_type_name STRING," +
                "  ts BIGINT " + SqlUtil.getKafkaSinkSql(TOPIC_DWD_TRADE_REFUND_PAY_SUC);

        env.executeSql(sinkSql);
        env.executeSql("insert into "+ TOPIC_DWD_TRADE_REFUND_PAY_SUC + joinSql);


    }
}
