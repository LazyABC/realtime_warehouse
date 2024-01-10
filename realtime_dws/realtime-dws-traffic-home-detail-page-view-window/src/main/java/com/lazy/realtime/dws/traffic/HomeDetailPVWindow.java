package com.lazy.realtime.dws.traffic;

import com.alibaba.fastjson.JSON;
import com.lazy.realtime.common.base.BaseDataStreamApp;
import com.lazy.realtime.common.base.BaseSqlApp;
import com.lazy.realtime.common.function.DorisMapFunction;
import com.lazy.realtime.common.util.DateTimeFormatUtil;
import com.lazy.realtime.common.util.DorisUtil;
import com.lazy.realtime.common.util.SqlUtil;
import com.lazy.realtime.dws.traffic.pojo.HomeDetailPVBean;
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
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.util.Collector;

import java.time.Duration;

import static com.lazy.realtime.common.constant.GmallConstant.DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRAFFIC_PAGE;

/**
 * @Name: Lazy
 * @Date: 2024/1/7 19:38:56
 * @Details: 流量域首页详情页页面浏览各窗口汇总表
 */
public class HomeDetailPVWindow extends BaseDataStreamApp {
    public static void main(String[] args) {

        new HomeDetailPVWindow()
                .start(
                        DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW,
                        12003,
                        4,
                        TOPIC_DWD_TRAFFIC_PAGE,
                        DWS_TRAFFIC_HOME_DETAIL_PAGE_VIEW_WINDOW
                );
    }
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //读数据，封装为pojo
        SingleOutputStreamOperator<HomeDetailPVBean> pojoDs = parseToPojo(ds);
        //开窗聚合
        SingleOutputStreamOperator<HomeDetailPVBean> aggDs = agg(pojoDs);
        //写出到Doris
        writeToDoris(aggDs);
    }

    private void writeToDoris(SingleOutputStreamOperator<HomeDetailPVBean> aggDs) {
        aggDs
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall_realtime.dws_traffic_home_detail_page_view_window"));
    }

    private SingleOutputStreamOperator<HomeDetailPVBean> agg(SingleOutputStreamOperator<HomeDetailPVBean> pojoDs) {

        WatermarkStrategy<HomeDetailPVBean> watermarkStrategy = WatermarkStrategy
                .<HomeDetailPVBean>forMonotonousTimestamps()
                .withTimestampAssigner( (e, ts) -> e.getTs() )
                .withIdleness(Duration.ofSeconds(10));

        return  pojoDs
                .assignTimestampsAndWatermarks(watermarkStrategy)
                .windowAll(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<HomeDetailPVBean>()
                {
                    @Override
                    public HomeDetailPVBean reduce(HomeDetailPVBean value1, HomeDetailPVBean value2) throws Exception {
                        value1.setHome_uv_ct(value1.getHome_uv_ct() + value2.getHome_uv_ct());
                        value1.setGood_detail_uv_ct(value1.getGood_detail_uv_ct() + value2.getGood_detail_uv_ct());
                        return value1;
                    }
                }, new AllWindowFunction<HomeDetailPVBean, HomeDetailPVBean, TimeWindow>()
                {
                    @Override
                    public void apply(TimeWindow window, Iterable<HomeDetailPVBean> values, Collector<HomeDetailPVBean> out) throws Exception {
                        HomeDetailPVBean result = values.iterator().next();
                        result.setStt(DateTimeFormatUtil.parseTsToDateTime(window.getStart()));
                        result.setEdt(DateTimeFormatUtil.parseTsToDateTime(window.getEnd()));
                        result.setCurDate(DateTimeFormatUtil.parseTsToDate(window.getStart()));
                        out.collect(result);
                    }
                });

    }

    private SingleOutputStreamOperator<HomeDetailPVBean> parseToPojo(DataStreamSource<String> ds) {

        return  ds
                .map(s -> JSON.parseObject(s, HomeDetailPVBean.class))
                .filter(b -> "home".equals(b.getPageId()) || "good_detail".equals(b.getPageId()))
                .keyBy(b -> b.getMid() + "_" +b.getPageId() )
                .process(new KeyedProcessFunction<String, HomeDetailPVBean, HomeDetailPVBean>()
                {
                    private ValueState<String> lastAccessDate;
                    //标准化处理，判断当前这条数据是否在今天下过单，是否在之前下过单

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        lastAccessDate = getRuntimeContext().getState(new ValueStateDescriptor<>("lastAccessDate", String.class));
                    }

                    @Override
                    public void processElement(HomeDetailPVBean value, Context ctx, Collector<HomeDetailPVBean> out) throws Exception {
                        String lastPayDateStr = lastAccessDate.value();
                        //获取业务日期
                        String eventDate = DateTimeFormatUtil.parseTsToDate(value.getTs() * 1000);

                        if (!eventDate.equals(lastPayDateStr)) {
                            if ("home".equals(value.getPageId())){
                                value.setHome_uv_ct(1l);

                            }else{
                                value.setGood_detail_uv_ct(1l);

                            }
                            out.collect(value);
                            lastAccessDate.update(eventDate);
                        }
                    }
                });
    }
}
