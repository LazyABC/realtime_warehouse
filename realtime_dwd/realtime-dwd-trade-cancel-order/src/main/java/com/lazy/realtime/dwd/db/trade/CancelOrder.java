package com.lazy.realtime.dwd.db.trade;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_CANCEL_DETAIL;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL;

/**
 * @Name: Lazy
 * @Date: 2024/1/3 18:55:37
 * @Details:
 *  取消订单的详情信息。
 *
 *         获取取消的订单order_info。
 *                 从order_info找update操作，更新order_status，从1001(下单)，更新为1003(取消)
 *             join
 *         获取下单的订单的详情.
 *             情形一： 不允许同层之间进行查询。
 *                         参考dwd_trade_order_detail.
 *                     离线数仓中，很多不允许同层查询。这涉及到调度的依赖关系。
 *
 *             情形二：  允许同层之间进行查询。
 *                         从dwd_trade_order_detail查询。
 */
public class CancelOrder extends BaseSqlApp {
    public static void main(String[] args) {
        new CancelOrder()
                .start(
                        TOPIC_DWD_TRADE_CANCEL_DETAIL,
                        11005,
                        4
                );
    }

    @Override
    protected void handle(TableEnvironment env) {
        //1.读ods层的原始数据(ods_db),名字叫ods_db
        createOdsDb(env);
        //2.获取取消的订单order_info
        String orderInfoSql = " select " +
                "  data['id'] id, " +
                "  data['operate_time'] cancel_time ," +
                "  ts " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'order_info' " +
                " and `type` = 'update' " +
                " and `old`['order_status'] = '1001' " +
                " and `data`['order_status'] = '1003'  ";

        env.createTemporaryView("oi",env.sqlQuery(orderInfoSql));


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
                "  user_id STRING, " +
                "  province_id STRING," +
                "  coupon_id STRING," +
                "   coupon_use_id STRING ," +
                "  activity_id STRING , " +
                "  `offset` BIGINT METADATA VIRTUAL  ," +
                "  activity_rule_id STRING  " + SqlUtil.getKafkaSourceSql(TOPIC_DWD_TRADE_ORDER_DETAIL,"Lazy");

        env.executeSql(dwdOrderDetailSql);

        //查询
        String dwdQuerySql = " select" +
                "  * ," +
                "  row_number() over(partition by id order by `offset` desc ) rn" +
                " from " + TOPIC_DWD_TRADE_ORDER_DETAIL ;

        env.createTemporaryView("od",env.sqlQuery(dwdQuerySql));

        //关联
        String joinSql = " select " +
                " tmp.id , " +
                " order_id , " +
                " sku_id  ," +
                " sku_num  ," +
                " split_total_amount  ," +
                " split_activity_amount  ," +
                " split_coupon_amount  ," +
                " create_time  ," +
                "  ts  ," +
                "  user_id , " +
                "  province_id ," +
                "  coupon_id ," +
                "  coupon_use_id  ," +
                "  activity_id  ," +
                "  activity_rule_id  , " +
                "  cancel_time " +
                "  from  oi " +
                "  join ( select * from od where rn = 1 ) tmp" +
                "  on oi.id = tmp.order_id ";

        /*
            创建sink表
                从TOPIC_DWD_TRADE_ORDER_DETAIL 关联查询，TOPIC_DWD_TRADE_ORDER_DETAIL有撤回，
                会造成我最终写出的结果也有撤回，必须用upsert-kafka写出
         */
        String sinkSql = " create table " + TOPIC_DWD_TRADE_CANCEL_DETAIL +"(" +
                " id STRING, " +
                " order_id STRING, " +
                " sku_id STRING ," +
                " sku_num STRING ," +
                " split_total_amount STRING ," +
                " split_activity_amount STRING ," +
                " split_coupon_amount STRING ," +
                " create_time STRING ," +
                " ts BIGINT ," +
                "  user_id STRING, " +
                "  province_id STRING," +
                "  coupon_id STRING," +
                "   coupon_use_id STRING ," +
                "  activity_id STRING ," +
                "  activity_rule_id STRING , " +
                "  cancel_time STRING ," +
                "   PRIMARY KEY (id) NOT ENFORCED " + SqlUtil.getUpsertKafkaSinkSql(TOPIC_DWD_TRADE_CANCEL_DETAIL);

        env.executeSql(sinkSql);
        env.executeSql("insert into "+ TOPIC_DWD_TRADE_CANCEL_DETAIL + joinSql);

    }
}




































