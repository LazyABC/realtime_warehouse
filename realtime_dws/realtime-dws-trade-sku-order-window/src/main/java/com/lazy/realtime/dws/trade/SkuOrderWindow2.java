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
public class SkuOrderWindow2 extends BaseSqlApp {

    public static void main(String[] args) {
        new SkuOrderWindow2()
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

        //2. 对数据按照offset进行升序排序   --------------------将offset改为create_time-----------------
        String rankSql = " select " +
                " * ," +
                " row_number() over(partition by id order by create_time ) rn " +
                " from t1 " ;
        //-------------------------------------------------------------------
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


        //4.关联维度表
        String joinSql = " select " +
                "    `stt`              ," +
                "    `edt`            ," +
                "    `cur_date` ," +
                "    cast(tm.id as SMALLINT)  `trademark_id` ," +
                "    tm.info.tm_name `trademark_name`," +
                "    cast(c1.id as SMALLINT)  category1_id   ," +
                "    c1.info.name `category1_name`  ," +
                "     cast(c2.id as SMALLINT)  category2_id   ," +
                "     c2.info.name `category2_name`  ," +
                "    cast(sku.info.category3_id as SMALLINT)  category3_id  ," +
                "     c3.info.name `category3_name`  ," +
                "    cast(sku.id as SMALLINT)  `sku_id`    ," +
                "    `sku_name`       ," +
                "    cast(spu.id as SMALLINT)  `spu_id`    ," +
                "    spu.info.spu_name `spu_name`  ," +
                "    cast(sku.info.price as decimal(16,2)) * sku_num  `original_amount` ," +
                "    `activity_reduce_amount`   ," +
                "    `coupon_reduce_amount`      ," +
                "    `order_amount`  " +
                " from t2 " +
                "  left JOIN sku FOR SYSTEM_TIME AS OF t2.pt " +
                "    ON t2.sku_id = sku.id " +
                "  left JOIN spu FOR SYSTEM_TIME AS OF t2.pt " +
                "    ON sku.spu_id = spu.id " +
                "  left JOIN tm FOR SYSTEM_TIME AS OF t2.pt " +
                "    ON sku.info.tm_id = tm.id " +
                "  left JOIN c3 FOR SYSTEM_TIME AS OF t2.pt " +
                "    ON sku.info.category3_id = c3.id " +
                "  left JOIN c2 FOR SYSTEM_TIME AS OF t2.pt " +
                "    ON c3.info.category2_id = c2.id " +
                "  left JOIN c1 FOR SYSTEM_TIME AS OF t2.pt " +
                "    ON c2.info.category1_id = c1.id " ;


        //5.结果写出到doris  数据类型的映射关系
        String sinkSql = " create table t2 (" +
                "    `stt`                    TIMESTAMP  ," +
                "    `edt`                    TIMESTAMP  ," +
                "    `cur_date`               DATE  ," +
                "    `trademark_id`           SMALLINT  ,"+
                "    `trademark_name`         STRING ,"  +
                "    `category1_id`           SMALLINT  ," +
                "    `category1_name`         STRING ," +
                "    `category2_id`           SMALLINT  ," +
                "    `category2_name`         STRING, " +
                "    `category3_id`           SMALLINT  ," +
                "    `category3_name`         STRING," +
                "    `sku_id`                 SMALLINT ," +
                "    `sku_name`               STRING ," +
                "    `spu_id`                 SMALLINT ," +
                "    `spu_name`               STRING, " +
                "    `original_amount`        DECIMAL(16, 2) ," +
                "    `activity_reduce_amount` DECIMAL(16, 2) ," +
                "    `coupon_reduce_amount`   DECIMAL(16, 2) ," +
                "    `order_amount`           DECIMAL(16, 2) "+
                SqlUtil.getDorisSinkSql("gmall_realtime.dws_trade_sku_order_window");

        env.executeSql(sinkSql);

        //env.sqlQuery("select * from t2").execute().print();


        //4.写出
        env.executeSql("insert into t2 " + joinSql);

    }
}
