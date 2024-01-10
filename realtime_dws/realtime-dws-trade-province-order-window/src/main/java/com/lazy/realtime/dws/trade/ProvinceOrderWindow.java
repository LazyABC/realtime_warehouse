package com.lazy.realtime.dws.trade;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.DWS_TRADE_PROVINCE_ORDER_WINDOW;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_ORDER_DETAIL;

/**
 * @Name: Lazy
 * @Date: 2024/1/9 14:26:49
 * @Details: 交易域省份粒度下单各窗口汇总表
 * 从Kafka读取订单明细数据，过滤null数据并按照唯一键对数据去重，
 * 统计各省份各窗口订单数和订单金额，将数据写入Doris交易域省份粒度下单各窗口汇总表
 */
public class ProvinceOrderWindow extends BaseSqlApp {
    public static void main(String[] args) {
        new ProvinceOrderWindow()
                .start(
                        DWS_TRADE_PROVINCE_ORDER_WINDOW,
                        12008,
                        4
                );

    }


    @Override
    protected void handle(TableEnvironment env) {

        //1.从dwd_order_detail 读取数据
        String odSql = " create table t1(" +
                " id STRING, " +
                " province_id STRING ," +
                " order_id STRING ," +
                " split_total_amount STRING ," +
                " create_time TIMESTAMP(0) ," +
                "  WATERMARK FOR create_time as create_time - INTERVAL '0.001' SECOND " +
                SqlUtil.getKafkaSourceSql(TOPIC_DWD_TRADE_ORDER_DETAIL,DWS_TRADE_PROVINCE_ORDER_WINDOW);

        env.executeSql(odSql);


        //env.createTemporaryView("t2",env.sqlQuery(" select * from t2 where rn = 1"));

        /*
            首次聚合，目的是去重。结果是每一个order_detail_id只有一行
                业务粒度的问题:
                        order_info:  一笔订单是一行。   单次 = 单数
                        order_detail:  一笔订单是N行。  单次 = 单数 = count(distinct order_id)
                                      一笔订单的一个商品是一行。

         */
        String tumbleSql = " SELECT " +
                "  window_start stt," +
                "  window_end edt," +
                "  id, " +
                "  province_id," +
                "   order_id ," +
                "  avg( cast(split_total_amount as decimal(16,2)) ) split_total_amount " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t1, DESCRIPTOR(create_time), INTERVAL '5' second )" +
                "   ) " +
                "  WHERE province_id is not null " +
                "  GROUP BY window_start, window_end , id  ,province_id ,order_id  ";

        env.createTemporaryView("t2",env.sqlQuery(tumbleSql));
        //第二次聚合，统计下单次数和下单金额
        String aggSql = " select " +
                " stt, edt,province_id, " +
                "  TO_DATE(DATE_FORMAT(stt,'yyyy-MM-dd')) cur_date," +
                "  sum(split_total_amount) order_amount ," +
                "  count(distinct order_id ) order_count ," +
                "  PROCTIME() pt " +
                " from t2 " +
                " group by stt, edt,province_id ";

        env.createTemporaryView("t3",env.sqlQuery(aggSql));


        //关联维度  使用look-upjoin
        String pSql = " create table p( " +
                "  id STRING ," +
                "  info Row<name STRING > ," +
                "  PRIMARY KEY (id) NOT ENFORCED  " +
                SqlUtil.getHBaseSourceSql("gmall:dim_base_province");

        env.executeSql(pSql);

        String joinSql = " select " +
                " `stt`                 ," +
                "`edt`                  ," +
                " `cur_date`             ," +
                " cast(province_id as SMALLINT) `province_id`  ," +
                " p.info.name `province_name`       ," +
                " order_count , " +
                "`order_amount` " +
                " from t3 " +
                " left join p FOR SYSTEM_TIME AS OF t3.pt " +
                " ON t3.province_id = p.id " ;

        //5.创建sink表
        String sinkSql = " create table t4 (" +
                "  `stt`            TIMESTAMP,       " +
                " `edt`                TIMESTAMP,   " +
                " `cur_date`         DATE,     " +
                " `province_id`     SMALLINT,     " +
                " `province_name`     STRING,   " +
                " `order_count`      BIGINT,    " +
                " `order_amount`     DECIMAL(16,2) "+SqlUtil.getDorisSinkSql("gmall_realtime.dws_trade_province_order_window");

        env.executeSql(sinkSql);
        env.executeSql("insert into t4 "+ joinSql);

    }
}
