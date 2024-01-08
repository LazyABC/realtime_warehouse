package com.lazy.realtime.dws.trade;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.DWS_TRADE_SKU_ORDER_WINDOW;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL;

/**
 * @Name: Lazy
 * @Date: 2024/1/8 18:23:01
 * @Details: 交易域SKU粒度下单各窗口汇总表
 * 从Kafka订单明细主题读取数据，过滤null数据并按照唯一键对数据去重，
 * 按照SKU维度分组，统计原始金额、活动减免金额、优惠券减免金额和订单金额，并关联维度信息，将数据写入Doris交易域SKU粒度下单各窗口汇总表
 */

public class SkuOrderWindow extends BaseSqlApp {


    public static void main(String[] args) {
        new SkuOrderWindow()
            .start(
                DWS_TRADE_SKU_ORDER_WINDOW,
                12008,
                4
            );
    }
    @Override
    protected void handle(TableEnvironment env) {

        //1.从dwd_order_detail 读取数据
        String odSql = " create table t1(" +
            " id STRING, " +
            " sku_id STRING ," +
            " sku_num STRING ," +
            " split_total_amount STRING ," +
            " split_activity_amount STRING ," +
            " split_coupon_amount STRING ," +
            " create_time TIMESTAMP(0) ," +
            "  `offset` BIGINT METADATA VIRTUAL  ," +
            " WATERMARK FOR create_time as create_time - INTERVAL '0.001' SECOND " +
            SqlUtil.getKafkaSourceSql(TOPIC_DWD_TRADE_ORDER_DETAIL,DWS_TRADE_SKU_ORDER_WINDOW);

        env.executeSql(odSql);

        /*
            2.由于dwd_order_detail 会出现同一个详情(order_detail_id)有多条数据的情况，因此要避免重复计算。

                解决方法一:
                            0. 先把sku_id 为null的过滤掉(省略)
                            1. 在窗口中，只对sku_id，order_detail_id 进行聚合
                              offset=5, id": "14245187",   sku_id=4, "province_id": null,
                              offset=6, id": "14245187",
                              offset=7, id": "14245187",   sku_id=4, "province_id": 1,
                            2. 采取 avg(split_total_amount)进行聚合。

                              offset=5, id": "14245187",   sku_id=4, split_total_amount = 10, "province_id": null,
                              offset=7, id": "14245187",   sku_id=4,split_total_amount = 10, "province_id": 1,
                            3. 还需要二次聚合
                                    在flink中二次聚合
                                        select  window_start,window_end,sku_id,sum(split_total_amount)  from tmp group by window_start,window_end,sku_id
                                        如何保证flink程序挂掉，Doris的结果不重复？
                                                Doris中value列选择 REPLACE
                                                可以不开启2PC提交。幂等输出。

                                    在Doris中二次聚合
                                        表模型必须是 aggregate，value列，要选择SUM类型

                                        如何保证flink程序挂掉，Doris的结果不重复？
                                                开启2PC提交。

                解决方法二:
                             0. 先把sku_id 为null的过滤掉
                             2. 在开窗前，对每条数据计算一个rn，按照时间(offset)升序排序

                              offset=5, id": "14245187",   sku_id=4, "province_id": null, rn = 1
                              offset=6, id": "14245187",                                   rn=2
                              offset=7, id": "14245187",   sku_id=4, "province_id": 1,      rn = 3
                            3.先过滤rn = 1 ,在窗口中，只对sku_id 进行聚合
                                offset=5, id": "14245187",   sku_id=4, "province_id": null, rn = 1
                                窗口中只需要一次聚合:  sum(split_total_amount)
         */

        //2. 对数据按照offset进行升序排序
        String rankSql = " select " +
            " * ," +
            " row_number() over(partition by id order by offset ) rn " +
            " from t1 " ;

        env.createTemporaryView("t2",env.sqlQuery(rankSql));

        //3.开窗计算
        String tumbleSql = " SELECT" +
            "  window_start," +
            "  window_end," +
            "  sku_id, " +
            "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
            "  sum(cast(sku_num as INT )) sku_num, " +
            "  sum( cast (split_activity_amount as decimal(16,2)) ) activity_reduce_amount, " +
            "  sum( cast (split_coupon_amount as decimal(16,2)) ) coupon_reduce_amount ," +
            "  sum( cast (split_total_amount as decimal(16,2)) ) order_amount " +
            "  FROM TABLE(" +
            "    TUMBLE(TABLE t2, DESCRIPTOR(create_time), INTERVAL '5' second )" +
            "   )" +
            "  WHERE rn = 1 " +
            "  GROUP BY window_start, window_end , sku_id ";

        //4.关联维度 使用look-upjoin
        String skuSql = " create table sku( " +
            "  id STRING ," +
            "  info Row<spu_id STRING,price STRING,sku_name STRING," +
            "   tm_id STRING, category3_id STRING > ," +
            "  PRIMARY KEY (id) NOT ENFORCED  " +
            SqlUtil.getHBaseSourceSql("gmall:dim_sku_info");

        String spuSql = " create table spu( " +
            "  id STRING ," +
            "  info Row<spu_name STRING> ," +
            "  PRIMARY KEY (id) NOT ENFORCED  " +
            SqlUtil.getHBaseSourceSql("gmall:dim_spu_info")
            ;

        String tmSql = " create table tm( " +
            "  id STRING ," +
            "  info Row<tm_name STRING> ," +
            "  PRIMARY KEY (id) NOT ENFORCED  " +
            SqlUtil.getHBaseSourceSql("gmall:dim_base_trademark")
            ;

        String c1Sql = " create table c1( " +
            "  id STRING ," +
            "  info Row<name STRING> ," +
            "  PRIMARY KEY (id) NOT ENFORCED  " +
            SqlUtil.getHBaseSourceSql("gmall:dim_base_category1")
            ;

        String c2Sql = " create table c2( " +
            "  id STRING ," +
            "  info Row<name STRING,category1_id STRING> ," +
            "  PRIMARY KEY (id) NOT ENFORCED  " +
            SqlUtil.getHBaseSourceSql("gmall:dim_base_category2")
            ;

        String c3Sql = " create table c3( " +
            "  id STRING ," +
            "  info Row<name STRING,category2_id STRING> ," +
            "  PRIMARY KEY (id) NOT ENFORCED  " +
            SqlUtil.getHBaseSourceSql("gmall:dim_base_category3")
            ;

        env.executeSql(skuSql);
        env.executeSql(spuSql);
        env.executeSql(tmSql);
        env.executeSql(c1Sql);
        env.executeSql(c2Sql);
        env.executeSql(c3Sql);




    }
}
