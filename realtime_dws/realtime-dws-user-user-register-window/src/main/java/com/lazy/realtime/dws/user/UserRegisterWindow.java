package com.lazy.realtime.dws.user;

import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.table.api.TableEnvironment;

import static com.lazy.realtime.common.constant.GmallConstant.DWS_USER_USER_REGISTER_WINDOW;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_USER_REGISTER;

/**
 * @Name: Lazy
 * @Date: 2024/1/8 11:33:20
 * @Details: 用户域用户注册各窗口汇总表
 */
public class UserRegisterWindow extends BaseSqlApp {
    public static void main(String[] args) {
        new UserRegisterWindow()
                .start(
                        DWS_USER_USER_REGISTER_WINDOW,
                        12005,
                        1
                );
    }

    @Override
    protected void handle(TableEnvironment env) {
        /*
            1.从dwd_user_register读取数据

            create_time是2023-12-11 15:34:58类型，可以直接使用 TIMESTAMP类型映射
         */
        String urSql = " create table t1(" +
                " create_time TIMESTAMP(0)," +
                " id STRING," +
                " WATERMARK FOR create_time as create_time - INTERVAL '0.001' SECOND " +
                SqlUtil.getKafkaSourceSql(TOPIC_DWD_USER_REGISTER,DWS_USER_USER_REGISTER_WINDOW);

        env.executeSql(urSql);

        //2.开窗计算
        String tumbleSql = " SELECT" +
                "  window_start," +
                "  window_end," +
                "  TO_DATE(DATE_FORMAT(window_start,'yyyy-MM-dd')) cur_date," +
                "  count(*) register_ct " +
                "  FROM TABLE(" +
                "    TUMBLE(TABLE t1, DESCRIPTOR(create_time), INTERVAL '5' second )" +
                "   )" +
                "  GROUP BY window_start, window_end  ";

        //3.创建sink表
        String sinkSql = " create table t2 (" +
                "   `stt` TIMESTAMP ," +
                "   `edt` TIMESTAMP ," +
                "   `cur_date`  DATE ," +
                "   `register_ct`   BIGINT " +
                SqlUtil.getDorisSinkSql("gmall_realtime.dws_user_user_register_window");

        env.executeSql(sinkSql);

        //env.sqlQuery("select * from t1").execute().print();

        //5.写出
        env.executeSql("insert into t2 " + tumbleSql);



    }
}
