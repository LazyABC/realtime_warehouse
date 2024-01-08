package com.lazy.realtime.dws.traffic;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRAFFIC_PAGE;

/**
 * @Name: Lazy
 * @Date: 2024/1/7 19:38:56
 * @Details: 流量域首页详情页页面浏览各窗口汇总表
 */
public class HomeDetailPVWindow extends BaseSqlApp {
    public static void main(String[] args) {

        new HomeDetailPVWindow()
                .start(
                        DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW,
                        12003,
                        4
                );

    }
    @Override
    protected void handle(TableEnvironment env) {
        //1.从dwd_page_log读取
        String dwdPageLogSql = " create table t1(" +
                " page_id STRING," +
                " mid STRING," +
                " ts BIGINT ," +
                " et as TO_TIMESTAMP_LTZ(ts,3) ," +
                " WATERMARK FOR et as et - INTERVAL '0.001' SECOND " +
                SqlUtil.getKafkaSourceSql(TOPIC_DWD_TRAFFIC_PAGE,"lazy");

        env.executeSql(dwdPageLogSql);

        //2.开窗计算
        String tumbleSql = " SELECT" +
                "  window_start," +
                "  window_end," +
                "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
                "  count(distinct if(page_id='home',mid,cast(null as string))) home_uv_ct," +
                "  count(distinct if(page_id='good_detail',mid,cast(null as string))) good_detail_uv_ct " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t1, DESCRIPTOR(et), INTERVAL '5' second )" +
                "   )" +
                "  GROUP BY window_start, window_end  ";

        //3.创建sink表
        String sinkSql = " create table t2 (" +
                "   `stt` TIMESTAMP ," +
                "   `edt` TIMESTAMP ," +
                "   `cur_date`  DATE ," +
                "   `home_uv_ct`   BIGINT ," +
                "   `good_detail_uv_ct` BIGINT "+SqlUtil.getDorisSinkSql("gmall_realtime.dws_traffic_home_detail_page_view_window");

        env.executeSql(sinkSql);

        //5.写出
        env.executeSql("insert into t2 " + tumbleSql);


    }
}
