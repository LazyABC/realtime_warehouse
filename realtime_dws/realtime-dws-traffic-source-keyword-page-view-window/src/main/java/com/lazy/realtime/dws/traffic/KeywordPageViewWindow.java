package com.lazy.realtime.dws.traffic;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import com.lazy.realtime.dws.traffic.function.SplitWordFunc;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRAFFIC_PAGE;

/**
 * @Name: Lazy
 * @Date: 2024/1/7 19:38:56
 * @Details: 流量域搜索关键词粒度页面浏览各窗口汇总表
 */
public class KeywordPageViewWindow extends BaseSqlApp
{
    public static void main(String[] args) {

        new KeywordPageViewWindow()
            .start(
                DWS_TRAFFIC_SOURCE_KEYWORD_PAGE_VIEW_WINDOW,
                12001,
                4
            );

    }
    @Override
    protected void handle(TableEnvironment env) {

        //1.从dwd_page_log读取数据
        String dwdPageLogSql = " create table "+ TOPIC_DWD_TRAFFIC_PAGE+"(" +
            " item_type STRING," +
            " item STRING," +
            " last_page_id STRING," +
            " ts BIGINT ," +
            " et as TO_TIMESTAMP_LTZ(ts,3) ," +
            " WATERMARK FOR et as et - INTERVAL '0.001' SECOND " +
            SqlUtil.getKafkaSourceSql(TOPIC_DWD_TRAFFIC_PAGE,"230724");

        env.executeSql(dwdPageLogSql);



        String queryKeyWord = " select " +
            " item ," +
            " et " +
            " from "+TOPIC_DWD_TRAFFIC_PAGE +
            " where (last_page_id = 'home' or last_page_id = 'search') " +
            " and item_type = 'keyword'" +
            " and item is not null ";

        env.createTemporaryView("kw",env.sqlQuery(queryKeyWord));

        /*
            2.切词
                小米手机
                    切分  小米
                         手机
                         小米手机
             中文切词软件 ik 切词器
         */
        env.createTemporaryFunction("splitword",new SplitWordFunc());
        String splitWordSql = " select" +
            " word," +
            " et " +
            " from kw " +
            " join LATERAL TABLE(splitword(item)) on true ";

        env.createTemporaryView("kws",env.sqlQuery(splitWordSql));

        //3.执行开窗  keyed事件时间的滚动窗口
        String tumbleSql = " SELECT" +
            "  window_start," +
            "  window_end," +
            "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
            "  word," +
            "  count(*) countnum " +
            "  FROM TABLE(" +
            "    TUMBLE(TABLE kws, DESCRIPTOR(et), INTERVAL '5' second )" +
            "   )" +
            "  GROUP BY window_start, window_end ,word ";

        //4.结果写出到doris  数据类型的映射关系
        // https://doris.apache.org/zh-CN/docs/dev/ecosystem/flink-doris-connector#doris-%E5%92%8C-flink-%E5%88%97%E7%B1%BB%E5%9E%8B%E6%98%A0%E5%B0%84%E5%85%B3%E7%B3%BB
        String sinkSql = " create table t1 (" +
            "   `stt` TIMESTAMP ," +
            "   `edt` TIMESTAMP ," +
            "   `cur_date`  DATE ," +
            "   `keyword`   STRING ," +
            "   `keyword_count` BIGINT "+SqlUtil.getDorisSinkSql("gmall_realtime.dws_traffic_source_keyword_page_view_window");

        env.executeSql(sinkSql);

        //5.写出
        env.executeSql("insert into t1 " + tumbleSql);


    }
}
