package com.lazy.realtime.dwd.db.trade;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_PAY_DETAIL_SUC;

/**
 * @Name: Lazy
 * @Date: 2024/1/5 13:55:52
 * @Details: 交易域支付成功事务事实表
 *
 * gmall:dim_base_dicgmall:dim_base_dic
 */
public class PaySucDetail extends BaseSqlApp
{
    public static void main(String[] args) {

        new PaySucDetail()
                .start(
                        TOPIC_DWD_TRADE_PAY_DETAIL_SUC,
                        11006,
                        4
                );

    }
    @Override
    protected void handle(TableEnvironment env) {
        //interval join无需设置ttl，系统会自动维护

        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDb(env);

        //2.查询支付成功的信息
        String paymentInfoSql = " select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['order_id'] order_id, " +
                "  data['trade_no'] trade_no, " +
                "  data['total_amount'] total_amount, " +
                "  data['payment_status'] payment_status, " +
                "  data['payment_type'] payment_type, " +
                "  data['callback_time'] callback_time," +
                "  ts ," +
                "  pt, " +
                "  et " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'payment_info' " +
                " and `type` = 'update'" +
                " and `old`['payment_status'] = '1601' " +
                " and `data`['payment_status'] = '1602' ";

        env.createTemporaryView("pi",env.sqlQuery(paymentInfoSql));


        //3.获取dwd_trade_order_detail
        String dwdOrderDetailSql = " create table " + TOPIC_DWD_TRADE_ORDER_DETAIL +"(" +
                " id STRING, " +
                " order_id STRING, " +
                " sku_id STRING ," +
                " sku_num STRING ," +
                " split_total_amount STRING ," +
                " split_activity_amount STRING ," +
                " split_coupon_amount STRING ," +
                " create_time STRING ," +
                "  province_id STRING," +
                "  coupon_id STRING," +
                "   coupon_use_id STRING ," +
                "  activity_id STRING , " +
                "  ts BIGINT ," +
                "  et as TO_TIMESTAMP_LTZ(ts,0) ," +
                "  `offset` BIGINT METADATA VIRTUAL  ," +
                "  activity_rule_id STRING  " + SqlUtil.getKafkaSourceSql(TOPIC_DWD_TRADE_ORDER_DETAIL,"230724");

        env.executeSql(dwdOrderDetailSql);

        //查询
        String dwdQuerySql = " select" +
                "  * ," +
                "  row_number() over(partition by id order by `offset` desc ) rn" +
                " from " + TOPIC_DWD_TRADE_ORDER_DETAIL ;

        env.createTemporaryView("od",env.sqlQuery(dwdQuerySql));


        //4.获取维度表  dim_dic_code
        createDimBaseDic(env);

        /*
            5.关联
                interval join： 必须有事件时间，和水印
                look-up join： 必须有处理时间
         */
        String joinSql = " select" +
                " pi.id, " +
                " user_id, " +
                " pi.order_id, " +
                " trade_no, " +
                " total_amount, " +
                " payment_status, " +
                " payment_type, " +
                " callback_time," +
                " sku_id  ," +
                " sku_num  ," +
                " split_total_amount  ," +
                " split_activity_amount  ," +
                " split_coupon_amount  ," +
                " create_time  ," +
                "  province_id ," +
                "  coupon_id ," +
                "  coupon_use_id  ," +
                "  activity_id  ," +
                "  activity_rule_id  , " +
                "  dim1.info.dic_name payment_status_name ,  " +
                "  dim2.info.dic_name payment_type_name ," +
                "  pi.ts " +
                " from pi " +
                " join (select * from od where rn = 1) tmp " +
                " on pi.order_id = tmp.order_id " +
                " and tmp.et between pi.et - INTERVAL '30' MINUTE and pi.et  " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF pi.pt as dim1 " +
                "    ON pi.payment_status = dim1.id " +
                "  left JOIN dim_dic_code FOR SYSTEM_TIME AS OF pi.pt as dim2 " +
                "    ON pi.payment_type = dim2.id ";




        //6.创建sink表
        String sinkSql = "create table " + TOPIC_DWD_TRADE_PAY_DETAIL_SUC + "("+
                "  id STRING, " +
                "  user_id STRING, " +
                "  order_id STRING, " +
                "  trade_no STRING, " +
                "  total_amount STRING, " +
                "  payment_status STRING, " +
                "  payment_type STRING, " +
                "  callback_time STRING," +
                "  sku_id  STRING ," +
                "  sku_num  STRING ," +
                "  split_total_amount  STRING ," +
                "  split_activity_amount  STRING ," +
                "  split_coupon_amount  STRING ," +
                "  create_time  STRING ," +
                "  province_id STRING ," +
                "  coupon_id STRING ," +
                "  coupon_use_id  STRING ," +
                "  activity_id  STRING ," +
                "  activity_rule_id   STRING, " +
                "  payment_status_name STRING , " +
                "  payment_type_name STRING ," +
                "  ts BIGINT," +
                "  PRIMARY KEY (id) NOT ENFORCED " + SqlUtil.getUpsertKafkaSinkSql(TOPIC_DWD_TRADE_PAY_DETAIL_SUC);



        env.executeSql(sinkSql);

        env.executeSql("insert into "+ TOPIC_DWD_TRADE_PAY_DETAIL_SUC + joinSql);


    }
}
