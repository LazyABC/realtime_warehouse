package com.lazy.realtime.dwd.db.trade;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import java.time.Duration;

import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND;

/**
 * @Name: Lazy
 * @Date: 2024/1/5 18:18:35
 * @Details:
 */
public class RefundOrder extends BaseSqlApp {

    public static void main(String[] args) {

        new RefundOrder()
                .start(
                        TOPIC_DWD_TRADE_ORDER_REFUND,
                        11007,
                        4
                );

    }

    @Override
    protected void handle(TableEnvironment env) {
        //当申请退单，商家同意后。order_refund_info和order_info的update是同步进行。
        env.getConfig().setIdleStateRetention(Duration.ofSeconds(10));

        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDb(env);

        //2.查询申请退单成功的退单信息
        String orderRefundSql = " select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['order_id'] order_id, " +
                "  data['sku_id'] sku_id, " +
                "  data['refund_type'] refund_type, " +
                "  data['refund_num'] refund_num, " +
                "  data['refund_amount'] refund_amount, " +
                "  data['refund_reason_type'] refund_reason_type," +
                "  data['refund_reason_txt'] refund_reason_txt," +
                "  data['operate_time'] operate_time," +
                "  ts ," +
                "  pt " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'order_refund_info' " +
                " and `type` = 'update'" +
                " and `old`['refund_status'] = '0701' " +
                " and `data`['refund_status'] = '0702' ";

        env.createTemporaryView("ro",env.sqlQuery(orderRefundSql));

        //3.获取退单的订单信息
        String orderInfoSql = " select " +
                "  data['id'] id, " +
                "  data['province_id'] province_id " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'order_info' " +
                " and `type` = 'update' " +
                " and `data`['order_status'] = '1005' "+
                " and `old`['order_status'] is not null "; //保证此次update，update是order_status字段

        env.createTemporaryView("oi",env.sqlQuery(orderInfoSql));


        //4.获取维度表  dim_dic_code
        createDimBaseDic(env);

        /*
            5.关联
         */
        String joinSql = " select" +
                " ro.id, " +
                " user_id, " +
                " order_id, " +
                " sku_id, " +
                " refund_type, " +
                " refund_num, " +
                " refund_amount, " +
                " refund_reason_type," +
                " refund_reason_txt," +
                " operate_time," +
                " province_id ," +
                "  dim1.info.dic_name refund_type_name  ,  " +
                "  dim2.info.dic_name refund_reason_type_name ," +
                "  ts " +
                " from ro " +
                " join oi " +
                " on ro.order_id = oi.id " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF ro.pt as dim1 " +
                "    ON ro.refund_type = dim1.id " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF ro.pt as dim2 " +
                "    ON ro.refund_reason_type = dim2.id ";

        //6.创建sink表
        String sinkSql = "create table "+TOPIC_DWD_TRADE_ORDER_REFUND+"("+
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
                "  refund_reason_type_name STRING  ," +
                "  ts BIGINT " + SqlUtil.getKafkaSinkSql(TOPIC_DWD_TRADE_ORDER_REFUND);

        env.executeSql(sinkSql);

        env.executeSql("insert into "+ TOPIC_DWD_TRADE_ORDER_REFUND + joinSql);


    }
}
