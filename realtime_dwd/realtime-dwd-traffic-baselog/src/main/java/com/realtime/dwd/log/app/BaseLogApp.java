package com.realtime.dwd.log.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.common.base.BaseDataStreamApp;
import com.lazy.realtime.common.util.DateTimeFormatUtil;
import com.lazy.realtime.common.util.KafkaUtil;
import com.lazy.realtime.common.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.ProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.flink.util.OutputTag;

import java.time.LocalDate;

import static com.lazy.realtime.common.constant.GmallConstant.*;

/**
 * @Name: Lazy
 * @Date: 2024/1/2 19:46:00
 * @Details: 用于处理和转换流日志数据的Flink应用程序，纠正有关新老用户的信息，并根据事件类型将结果写入不同的Kafka主题
 */
@Slf4j
public class BaseLogApp extends BaseDataStreamApp {
    public static void main(String[] args) {
        new BaseLogApp()
                .start(
                        "dwd_traffic_baselog",
                        11001,
                        4,
                        PropertyUtil.getStringValue("TOPIC_ODS_LOG"),
                        "Lazy"
                );
    }

    /*
        1.etl只处理start，page，actions，displays，err这五种数据
            如果包含以上五种数据，且有内容，就留下

        2.修正新老用户的错误。
            把一部分isnew=1的老用户进行重置。

        3.分流写出
            把五种写不同的事实，展开明细后，写入到五个不同的主题
     */
    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //1.etl
        SingleOutputStreamOperator<String> etledDs = etl(ds);

        //2.新老用户的纠正
        SingleOutputStreamOperator<JSONObject> correctDs = correctNewUser(etledDs);

        //3.分流，展开明细，写出到kafka的不同的主题中保存
        writeDataToKafka(correctDs);

        correctDs.printToErr();
    }

    private void writeDataToKafka (SingleOutputStreamOperator<JSONObject> correctDs){
        OutputTag<String> pageTag = new OutputTag<>(PAGE, Types.STRING);
        OutputTag<String> errTag = new OutputTag<>(ERR, Types.STRING);
        OutputTag<String> actionTag = new OutputTag<>(ACTIONS, Types.STRING);
        OutputTag<String> displayTag = new OutputTag<>(DISPLAYS, Types.STRING);
        /*
            展开明细后分流:

            需要什么明细:
                    公共明细: common,ts
            start:  start{}
            page:   page{}
            display:  把整个displays[]数组中，每一个曝光事件，都作为一条数据写出。
                        做炸裂操作。
                        display,page
            action:  把整个actions[]数组中，每一个动作事件，都作为一条数据写出。
                        做炸裂操作。
                        action,page。
                         需要把ts替换为 action中的ts
            err:    err{}, start|page
         */

        SingleOutputStreamOperator<String> mainDs = correctDs
                .process(new ProcessFunction<JSONObject, String>() {
                    /**
                     *
                     * @param page  已经包含了common，ts，和page信息的{}
                     *                  {pageid:xxx,ts:xxx,mid:xxx}
                     * @param value 原始数据
                     *                  { actions:[ {actionId:1,ts:1},{actionId:2,ts:2} ]  }
                     * @param type  actions|displays
                     * @param ctx  负责用侧流写出到下游
                     * @param tag  输出标记
                     */
                    private void writeDisplayOrActionData(JSONObject page, JSONObject value, String type,
                                                          ProcessFunction<JSONObject, String>.Context ctx,
                                                          OutputTag<String> tag) {

                        //[ {actionId:1},{actionId:2} ]
                        JSONArray jsonArray = value.getJSONArray(type);

                        for (int i = 0; i < jsonArray.size(); i++) {
                            //{actionId:1}
                            JSONObject jsonObject = jsonArray.getJSONObject(i);
                            Long actionTs = jsonObject.getLong("ts");
                            jsonObject.putAll(page);
                            if (ACTIONS.equals(type)) {
                                jsonObject.put("ts", actionTs);
                            }
                            //侧流写出
                            ctx.output(tag, jsonObject.toJSONString());
                        }
                    }

                    /**
                     *
                     * @param common  原始数据的common部分  {"is_new":"1","mid":"mid_1234567"}
                     * @param ts       原始数据的ts部分   1703320089500
                     * @param value   原始数据
                     *                {"common":{"is_new":"1","mid":"mid_1234567"},"actions": [],"page": { "during_time": 9327},"ts": 1703320089500}
                     * @param type    封装的属性名，可以是 page,start,err中的一个
                     * @return 本质是抽取感兴趣的部分的数据，扁平化为{}返回
                     */
                    private JSONObject parseData(JSONObject common, Long ts, JSONObject value, String type) {
                        //{"is_new":"1","mid":"mid_1234567","ts":1703320089500}
                        common.put("ts", ts);
                        //type=page ,typeJO = { "during_time": 9327}
                        JSONObject typeJO = value.getJSONObject(type);
                        //合并属性，把common中的属性，和type中的属性，合并为一个Map
                        //{"is_new":"1","mid":"mid_1234567","ts":1703320089500,"during_time": 9327}
                        common.putAll(typeJO);
                        return common;
                    }

                    @Override
                    public void processElement(JSONObject value, ProcessFunction<JSONObject, String>.Context ctx, Collector<String> out) throws Exception {

                        JSONObject common = value.getJSONObject("common");
                        Long ts = value.getLong("ts");

                        if (value.containsKey(START)) {
                            //当前是启动日志
                            JSONObject startJO = parseData(common, ts, value, START);
                            out.collect(startJO.toJSONString());
                        } else {
                            //不是启动日志，就一定是页面访问日志
                            JSONObject pageJO = parseData(common, ts, value, PAGE);
                            ctx.output(pageTag, pageJO.toJSONString());

                            //既包含页面信息，还有页面发生的曝光
                            if (value.containsKey(DISPLAYS)) {
                                writeDisplayOrActionData(pageJO, value, DISPLAYS, ctx, displayTag);
                            }

                            //既包含页面信息，还有页面发生的动作
                            if (value.containsKey(ACTIONS)) {
                                writeDisplayOrActionData(pageJO, value, ACTIONS, ctx, actionTag);
                            }

                        }

                        //不管是启动，还是页面，都可能报错
                        if (value.containsKey(ERR)) {
                            JSONObject errJO = parseData(common, ts, value, ERR);
                            if (value.containsKey(START)) {
                                errJO.putAll(value.getJSONObject(START));
                            }
                            if (value.containsKey(PAGE)) {
                                errJO.putAll(value.getJSONObject(PAGE));
                            }
                            ctx.output(errTag, errJO.toJSONString());
                        }
                    }
                });

        mainDs.sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_START));
        //获得测流
        mainDs.getSideOutput(pageTag).sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_PAGE));
        mainDs.getSideOutput(errTag).sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_ERR));
        mainDs.getSideOutput(displayTag).sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_DISPLAY));
        mainDs.getSideOutput(actionTag).sinkTo(KafkaUtil.getKafkaSink(TOPIC_DWD_TRAFFIC_ACTION));

    }

    /*
        为每个设备(用户)，保存一个首日访问日期。
            通过数据中的is_new和首日访问日期，判断当前用户是否是老用户，但是is_new误判，进行修正。
     */
    private SingleOutputStreamOperator<JSONObject> correctNewUser (SingleOutputStreamOperator<String> etledDs){
        return etledDs
                .keyBy(jsonstr -> JSON.parseObject(jsonstr).getJSONObject("common").getString("mid"))
                .process(new KeyedProcessFunction<String, String, JSONObject>()
                {

                    private ValueState<String> firstVisitDate;

                    @Override
                    public void open(Configuration parameters) throws Exception {
                        firstVisitDate = getRuntimeContext().getState(new ValueStateDescriptor<>("dt", String.class));
                    }

                    @Override
                    public void processElement(String jsonStr, Context ctx, Collector<JSONObject> out) throws Exception {

                        JSONObject originalData = JSON.parseObject(jsonStr);
                        JSONObject common = originalData.getJSONObject("common");
                        Long ts = originalData.getLong("ts");
                        //当前的访问日期
                        String logDate = DateTimeFormatUtil.parseTsToDate(ts);
                        //获取is_new和firstVisitDate
                        String is_new = common.getString("is_new");
                        String firstVisitDateValue = firstVisitDate.value();
                        String mid = ctx.getCurrentKey();

                        //判断 firstVisitDateValue是不是null
                        if (firstVisitDateValue == null) {
                            if (ISNEWOLD.equals(is_new)) {
                                log.warn("当前客户:" + mid + "，是一个老用户，缺失首次访问日期...");
                                firstVisitDate.update(LocalDate.parse(logDate).plusDays(-1).toString());
                            } else {
                                log.warn("当前客户:" + mid + "，是一个新用户，这是他的首次访问...");
                                firstVisitDate.update(logDate);
                            }
                        } else if (
                                LocalDate.parse(firstVisitDateValue).isBefore(LocalDate.parse(logDate))
                                        &&
                                        ISNEWNEW.equals(is_new)
                        ) {
                            //明明是老客户，但是is_new却标记为1，需要修正
                            log.warn("当前客户:" + mid + "，是一个老用户，修正is_new...");
                            common.put("is_new", ISNEWOLD);
                        }

                        out.collect(originalData);
                    }
                });
    }


    //过滤数据，检查JSON数据是否包含特定的键，如"start","page","err","displays","actions" ，如果包含其中之一且非空，则通过过滤器
    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> ds) {
        /*
           日志格式 {}
         */
        return ds
                .filter(new FilterFunction<String>() {
                    @Override
                    public boolean filter(String json) throws Exception {

                        try {
                        JSONObject jsonObject = JSON.parseObject(json);
                        /*
                            判断是不是start,page,displays,actions,err其中的一种
                            start : {}
                            page : {}
                            err : {}
                            displays : []
                            actions : []
                            displays 和 actions 可能为[]。
                         */

                        return
                                        (jsonObject.containsKey(START) && jsonObject.getString(START).length() > 2) ||
                                        (jsonObject.containsKey(PAGE) && jsonObject.getString(PAGE).length() > 2) ||
                                        (jsonObject.containsKey(ERR) && jsonObject.getString(ERR).length() > 2) ||
                                        (jsonObject.containsKey(DISPLAYS) && JSON.parseArray(jsonObject.getString(DISPLAYS)).size() > 0) ||
                                        (jsonObject.containsKey(ACTIONS) && JSON.parseArray(jsonObject.getString(ACTIONS)).size() > 0);

                        } catch (Exception e){
                        //不是完整的json格式
                            log.warn(json + "当前数据不是json格式");
                            return false;
                        }
                    }
                });

    }
}
