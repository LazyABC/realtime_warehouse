package com.lazy.realtime.dwd.db.trade;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL;

/**
 * @Name: Lazy
 * @Date: 2024/1/3 15:32:40
 * @Details:
 * 业务过程： 下单。
 *     涉及四张表:
 *         order_info: 订单表。 向这个表insert一条记录。
 *                 一个订单是一行。
 *                 获取user_id，province_id
 *         order_detail(主): 订单详情表。 向这个表insert N条记录
 *                 一个订单的一个商品是一行。
 *         order_detail_activity： 订单商品参与活动的优惠信息表
 *                   一个订单的一个商品是一行。
 *                     获取参与的活动的信息
 *          order_detail_coupon： 订单商品使用优惠券的优惠信息表
 *                  一个订单的一个商品是一行。
 *                    获取使用的优惠券的信息
 */
public class OrderDetail extends BaseSqlApp {
    public static void main(String[] args) {
        new OrderDetail()
                .start(
                        TOPIC_DWD_TRADE_ORDER_DETAIL,
                        11004,
                        4
                );
    }
    @Override
    protected void handle(TableEnvironment env) {
        //1.读ods层的原始数据(ods_db),名字ods_db
        createOdsDb(env);
        //2.创建4张和下单相关的业务表
        String orderInfoSql = " select " +
                "  data['id'] id, " +
                "  data['user_id'] user_id, " +
                "  data['province_id'] province_id " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'order_info' " +
                " and `type` = 'insert' ";

        String orderDetailSql = " select " +
                "  data['id'] id, " +
                "  data['order_id'] order_id, " +
                "  data['sku_id'] sku_id ," +
                "  data['sku_num'] sku_num ," +
                "  data['split_total_amount'] split_total_amount ," +
                "  data['split_activity_amount'] split_activity_amount ," +
                "  data['split_coupon_amount'] split_coupon_amount ," +
                "  data['create_time'] create_time ," +
                "  ts " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'order_detail' " +
                " and `type` = 'insert' ";

        String couponDetailSql = " select " +
                "  data['order_detail_id'] order_detail_id, " +
                "  data['coupon_id'] coupon_id ," +
                "  data['coupon_use_id'] coupon_use_id " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'order_detail_coupon' " +
                " and `type` = 'insert' ";

        String activityDetailSql = " select " +
                "  data['order_detail_id'] order_detail_id, " +
                "  data['activity_id'] activity_id ," +
                "  data['activity_rule_id'] activity_rule_id " +
                " from ods_db " +
                " where `database` = 'gmall' " +
                " and `table` = 'order_detail_activity' " +
                " and `type` = 'insert' ";

        env.createTemporaryView("od",env.sqlQuery(orderDetailSql));
        env.createTemporaryView("oi",env.sqlQuery(orderInfoSql));
        env.createTemporaryView("cd",env.sqlQuery(couponDetailSql));
        env.createTemporaryView("ad",env.sqlQuery(activityDetailSql));

        //3.关联
        String joinSql = " select " +
                "  od.* , " +
                "  user_id, " +
                "  province_id," +
                "  coupon_id," +
                "   coupon_use_id ," +
                "  activity_id ," +
                "  activity_rule_id " +
                " from od " +
                " left join oi on od.order_id = oi.id " +
                " left join cd on od.id = cd.order_detail_id " +
                " left join ad on od.id = ad.order_detail_id ";

        /*
            4.创建sink表
                    left join: 结果可能会有 -D，不能使用普通的kafka连接器写出，只能使用upsert-kafka连接器。
                        upsert-kafka必须要求有主键约束。
                            主键作为 Record中的key。

                            +I:   Record(K,V)
                            -D:   Record(K,null)
         */
        String sinkSql = " create table " + TOPIC_DWD_TRADE_ORDER_DETAIL +"(" +
                " id STRING, " +
                " order_id STRING, " +
                " sku_id STRING ," +
                " sku_num STRING ," +
                " split_total_amount STRING ," +
                " split_activity_amount STRING ," +
                " split_coupon_amount STRING ," +
                " create_time STRING ," +
                "  ts BIGINT ," +
                "  user_id STRING, " +
                "  province_id STRING," +
                "  coupon_id STRING," +
                "   coupon_use_id STRING ," +
                "  activity_id STRING ," +
                "  activity_rule_id STRING , " +
                "   PRIMARY KEY (id) NOT ENFORCED " + SqlUtil.getUpsertKafkaSinkSql(TOPIC_DWD_TRADE_ORDER_DETAIL);

        env.executeSql(sinkSql);

        //写出
        env.executeSql("insert into "+ TOPIC_DWD_TRADE_ORDER_DETAIL + joinSql);
    }
}
