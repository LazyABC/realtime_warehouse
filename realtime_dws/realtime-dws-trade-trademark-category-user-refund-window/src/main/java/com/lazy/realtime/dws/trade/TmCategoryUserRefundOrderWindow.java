package com.lazy.realtime.dws.trade;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.common.base.BaseDataStreamApp;
import com.lazy.realtime.common.function.AsyncLookUpJoinFunction;
import com.lazy.realtime.common.function.DorisMapFunction;
import com.lazy.realtime.common.function.LookUpJoinFunction;
import com.lazy.realtime.common.util.DateTimeFormatUtil;
import com.lazy.realtime.common.util.DorisUtil;
import com.lazy.realtime.dws.trade.pojo.TmCategoryUserRefundOrderBean;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.streaming.api.datastream.AsyncDataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static com.lazy.realtime.common.constant.GmallConstant.DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW;
import static com.lazy.realtime.common.constant.GmallConstant.TOPIC_DWD_TRADE_ORDER_REFUND;

/**
 * @Name: Lazy
 * @Date: 2024/1/9 14:52:31
 * @Details: 交易域品牌-品类-用户粒度退单各窗口汇总表
 * 从Kafka读取订单明细数据，过滤null数据并按照唯一键对数据去重，
 * 统计各省份各窗口订单数和订单金额，将数据写入Doris交易域省份粒度下单各窗口汇总表
 */
public class TmCategoryUserRefundOrderWindow extends BaseDataStreamApp {
    public static void main(String[] args) {
        new TmCategoryUserRefundOrderWindow()
                .start(
                        DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW,
                        12010,
                        4,
                        TOPIC_DWD_TRADE_ORDER_REFUND,
                        DWS_TRADE_TRADEMARK_CATEGORY_USER_REFUND_WINDOW
                );

    }


    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //根据skuId,关联维度，封装数据为POJO
        SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> pojoDs = parseToPojo(ds);
        //关联维度
        SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> dimOpDs = getDimDataAsync(pojoDs);
        dimOpDs.print();
        //开窗聚合
        //SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> aggDs = agg(dimOpDs);
        //写出
        //writeToDoris(aggDs);
    }

    /*
        异步关联维度。
            1.flink中调用的函数，必须支持异步IO，是AsyncFunction的子类
            2.需要使用支持异步IO的Redis客户端
            3.需要使用支持异步IO的HBase客户端
     */

    private SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> getDimDataAsync(SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> pojoDs) {

        //调用AsyncFunction类型的函数，必须使用固定的套路
        SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> skuDs = AsyncDataStream
                .orderedWait(
                        pojoDs,
                        new AsyncLookUpJoinFunction<TmCategoryUserRefundOrderBean>("sku_info")
                        {

                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getSkuId();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory3Id(dimData.getString("category3_id"));
                                value.setTrademarkId(dimData.getString("tm_id"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );

        SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> tmDs = AsyncDataStream
                .orderedWait(
                        skuDs,
                        new AsyncLookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_trademark")
                        {

                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getTrademarkId();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setTrademarkName(dimData.getString("tm_name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );

        SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> c3Ds = AsyncDataStream
                .orderedWait(
                        tmDs,
                        new AsyncLookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_category3")
                        {

                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getCategory3Id();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory3Name(dimData.getString("name"));
                                value.setCategory2Id(dimData.getString("category2_id"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );

        SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> c2Ds = AsyncDataStream
                .orderedWait(
                        c3Ds,
                        new AsyncLookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_category2")
                        {

                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getCategory2Id();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory2Name(dimData.getString("name"));
                                value.setCategory1Id(dimData.getString("category1_id"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );

        return AsyncDataStream
                .orderedWait(
                        c2Ds,
                        new AsyncLookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_category1")
                        {

                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getCategory1Id();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory1Name(dimData.getString("name"));
                            }
                        },
                        120,
                        TimeUnit.SECONDS
                );


    }
        private SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> getDimData(SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> pojoDs) {
        return
                pojoDs
                        //使用sku_id 关联 sku_info
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("sku_info")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getSkuId();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory3Id(dimData.getString("category3_id"));
                                value.setTrademarkId(dimData.getString("tm_id"));
                            }
                        })
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_trademark")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getTrademarkId();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setTrademarkName(dimData.getString("tm_name"));
                            }
                        })
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_category3")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getCategory3Id();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory3Name(dimData.getString("name"));
                                value.setCategory2Id(dimData.getString("category2_id"));
                            }
                        })
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_category2")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getCategory2Id();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory2Name(dimData.getString("name"));
                                value.setCategory1Id(dimData.getString("category1_id"));
                            }
                        })
                        .map(new LookUpJoinFunction<TmCategoryUserRefundOrderBean>("base_category1")
                        {
                            @Override
                            public String getIdValue(TmCategoryUserRefundOrderBean value) {
                                return value.getCategory1Id();
                            }

                            @Override
                            protected void extractDimData(TmCategoryUserRefundOrderBean value, JSONObject dimData) {
                                value.setCategory1Name(dimData.getString("name"));

                            }
                        });
    }

    private void writeToDoris(SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> aggDs) {
        aggDs
                .map(new DorisMapFunction<>())
                .sinkTo(DorisUtil.getDorisSink("gmall_realtime.dws_trade_trademark_category_user_refund_window"));
    }

    private SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> agg(SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> pojoDs) {
        return pojoDs
                .keyBy(b -> b.getTrademarkId() + "_" + b.getCategory3Id() + "_" + b.getUserId())
                .window(TumblingEventTimeWindows.of(Time.seconds(5)))
                .reduce(new ReduceFunction<TmCategoryUserRefundOrderBean>()
                        {
                            @Override
                            public TmCategoryUserRefundOrderBean reduce(TmCategoryUserRefundOrderBean value1, TmCategoryUserRefundOrderBean value2) throws Exception {
                                value1.setRefundCount(value1.getRefundCount() + value2.getRefundCount());
                                return value1;
                            }
                        },
                        new ProcessWindowFunction<TmCategoryUserRefundOrderBean, TmCategoryUserRefundOrderBean, String, TimeWindow>()
                        {
                            @Override
                            public void process(String key, Context context, Iterable<TmCategoryUserRefundOrderBean> elements, Collector<TmCategoryUserRefundOrderBean> out) throws Exception {
                                TmCategoryUserRefundOrderBean result = elements.iterator().next();
                                TimeWindow window = context.window();
                                result.setStt(DateTimeFormatUtil.parseTsToDateTime(window.getStart()));
                                result.setEdt(DateTimeFormatUtil.parseTsToDateTime(window.getEnd()));
                                result.setCurDate(DateTimeFormatUtil.parseTsToDate(window.getStart()));
                                out.collect(result);
                            }
                        });
    }

    private SingleOutputStreamOperator<TmCategoryUserRefundOrderBean> parseToPojo(DataStreamSource<String> ds) {

        WatermarkStrategy<TmCategoryUserRefundOrderBean> watermarkStrategy = WatermarkStrategy
                .<TmCategoryUserRefundOrderBean>forMonotonousTimestamps()
                .withTimestampAssigner( (e, ts) -> e.getTs() * 1000)
                .withIdleness(Duration.ofSeconds(10));

        return  ds
                .map(s -> JSON.parseObject(s, TmCategoryUserRefundOrderBean.class))
                .assignTimestampsAndWatermarks(watermarkStrategy);

    }
}
