package com.lazy.realtime.dws.trade;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.DWS_TRADE_CART_ADD_UU_WINDOW;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_CART_ADD;

/**
 * @Name: Lazy
 * @Date: 2024/1/8 14:27:42
 * @Details: 交易域加购各窗口汇总表
 * 从 Kafka 读取用户加购明细数据，统计各窗口加购独立用户数，写入 Doris。
 */
public class CartAddWindow extends BaseSqlApp {

    public static void main(String[] args) {
        new CartAddWindow()
                .start(
                        DWS_TRADE_CART_ADD_UU_WINDOW,
                        12003,
                        4
                );
    }


    @Override
    protected void handle(TableEnvironment env) {
        /*
            1.从dwd_cart_add读取数据

         */
        String urSql = " create table t1(" +
                " ts BIGINT," +
                " user_id STRING," +
                " et as TO_TIMESTAMP_LTZ(ts,0) ," +
                " WATERMARK FOR et as et - INTERVAL '0.001' SECOND " +
                SqlUtil.getKafkaSourceSql(TOPIC_DWD_TRADE_CART_ADD,DWS_TRADE_CART_ADD_UU_WINDOW);

        env.executeSql(urSql);

        //2.开窗计算
        String tumbleSql = " SELECT" +
                "  window_start," +
                "  window_end," +
                "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
                "  count(distinct user_id) cart_add_uu_ct " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t1, DESCRIPTOR(et), INTERVAL '5' second )" +
                "   )" +
                "  GROUP BY window_start, window_end  ";

        //3.创建sink表
        String sinkSql = " create table t2 (" +
                "   `stt` TIMESTAMP ," +
                "   `edt` TIMESTAMP ," +
                "   `cur_date`  DATE ," +
                "   `cart_add_uu_ct`   BIGINT " +
                SqlUtil.getDorisSinkSql("gmall_realtime.dws_trade_cart_add_uu_window");

        env.executeSql(sinkSql);

        //5.写出
        env.executeSql("insert into t2 " + tumbleSql);

    }
}
