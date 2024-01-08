package com.lazy.realtime.dws.user;

import com.alibaba.fastjson.JSON;
import com.lazy.realtime.common.base.BaseDataStreamApp;
import com.lazy.realtime.common.function.DorisMapFunction;
import com.lazy.realtime.common.util.DateTimeFormatUtil;
import com.lazy.realtime.common.util.DorisUtil;
import com.lazy.realtime.dws.user.pojo.UserLoginBean;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.AllWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.lazy.realtime.common.constant.GmallConstant.DWS_USER_USER_LOGIN_WINDOW;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRAFFIC_PAGE;

/**
 * @Name: Lazy
 * @Date: 2024/1/7 19:46:46
 * @Details:  用户域用户登录各窗口汇总表
 */
public class UserLoginWindow extends BaseDataStreamApp {
    public static void main(String[] args) {

        new UserLoginWindow()
                .start(
                        DWS_USER_USER_LOGIN_WINDOW,
                        12004,
                        4,
                        TOPIC_DWD_TRAFFIC_PAGE,
                        DWS_USER_USER_LOGIN_WINDOW
                );

    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {

        //解析为pojo，并进行标准化处理
        SingleOutputStreamOperator<UserLoginBean> pojoDs = parseToPojo(ds);
        //开窗聚合
        SingleOutputStreamOperator<UserLoginBean> aggDs = agg(pojoDs);
        //写出到Doris
        writeTODoris(aggDs);
    }

    private void writeTODoris(SingleOutputStreamOperator<UserLoginBean> aggDs) {
        aggDs
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall_realtime.dws_user_user_login_window"));
    }

    private SingleOutputStreamOperator<UserLoginBean> agg(SingleOutputStreamOperator<UserLoginBean> pojoDs) {
        return pojoDs
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<UserLoginBean>()
                        {
                            @Override
                            public UserLoginBean reduce(UserLoginBean value1, UserLoginBean value2) throws Exception {
                                value2.setUuCt(value1.getUuCt() + value2.getUuCt());
                                value2.setBackCt(value1.getBackCt() + value2.getBackCt());
                                return value2;
                            }
                        }, new AllWindowFunction<UserLoginBean, UserLoginBean, TimeWindow>()
                        {
                            @Override
                            public void apply(TimeWindow window, Iterable<UserLoginBean> values, Collector<UserLoginBean> out) throws Exception {

                                UserLoginBean u = values.iterator().next();
                                u.setStt(DateTimeFormatUtil.parseTsToDateTime(window.getStart()));
                                u.setEdt(DateTimeFormatUtil.parseTsToDateTime(window.getEnd()));
                                //[2024-01-06 23:59:55,2024-01-07 00:00:00)  使用starttime获取统计日期
                                u.setCurDate(DateTimeFormatUtil.parseTsToDate(window.getStart()));
                                out.collect(u);
                            }
                        }
                );

    }

    private SingleOutputStreamOperator<UserLoginBean> parseToPojo(DataStreamSource<String> ds) {

        WatermarkStrategy<UserLoginBean> watermarkStrategy = WatermarkStrategy
                .<UserLoginBean>forMonotonousTimestamps()
                .withTimestampAssigner( (e, ts) -> e.getTs())
                .withIdleness(Duration.ofSeconds(30));
        /*
              `back_ct`  BIGINT REPLACE COMMENT '回流用户数',
                lastVisitDate=null,  uid=1,ts=1,eventDate=2024-01-04,login,backCt=0
                 lastVisitDate=2024-01-04,   uid=1,ts=2,eventDate=2024-01-04,login,backCt=0
                 lastVisitDate=2024-01-04,  uid=1,ts=3,eventDate=2024-01-04,login,backCt=0

              `uu_ct`    BIGINT REPLACE COMMENT '独立用户数'
                 uid=1,ts=1,eventDate=2024-01-04,login,uuCt=1
                 uid=1,ts=2,eventDate=2024-01-04,login,uuCt=0，backCt=0
                 uid=1,ts=3,eventDate=2024-01-04,login,uuCt=0
                 uid=1,ts=4,eventDate=2024-01-04,login,uuCt=0

         */
        return ds
                .map(s -> JSON.parseObject(s, UserLoginBean.class))
                //uid不能为null ,last_page_id要么为null，要么为page
                .filter(u -> StringUtils.isNotBlank(u.getUid()) &&
                        ("page".equals(u.getLast_page_id()) || u.getLast_page_id() != null))
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .keyBy(UserLoginBean::getUid)
                .process(new KeyedProcessFunction<String, UserLoginBean, UserLoginBean>()
                {
                    //维护一个最近访问日期，用来判断当前用户是否是回流的
                    private ValueState<String> lastVisitDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastVisitDate = getRuntimeContext().getState(new ValueStateDescriptor<>("lastVisitDate", String.class));
                    }

                    @Override
                    public void processElement(UserLoginBean value, Context ctx, Collector<UserLoginBean> out) throws Exception {
                        String lastVisitDateStr = lastVisitDate.value();
                        long ts = value.getTs();
                        String eventDate = DateTimeFormatUtil.parseTsToDate(ts);

                    /*
                        不相等:
                          lastVisitDate = null,这是当前用户当天第一次访问。
                           lastVisitDate != null,跨天，这是当前用户当天第一次访问。
                     */
                        if (!eventDate.equals(lastVisitDateStr)) {
                            value.setUuCt(1l);
                            //更新lastVisitDate
                            lastVisitDate.update(eventDate);

                            if (lastVisitDateStr != null) {
                                //之前来过
                                long diffDays = TimeUnit.DAYS.convert(ts - DateTimeFormatUtil.parseDateToTs(lastVisitDateStr), TimeUnit.MILLISECONDS);
                                if (diffDays > 7) {
                                    //最近7天没来，现在来了，符合回流条件
                                    value.setBackCt(1l);
                                }
                            }
                        }

                        //发送到下游
                        if (value.getUuCt() > 0) {
                            out.collect(value);
                        }
                    }
                });

    }
}

