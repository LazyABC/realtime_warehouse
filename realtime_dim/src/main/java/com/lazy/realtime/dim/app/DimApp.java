package com.lazy.realtime.dim.app;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.google.common.collect.Maps;
import com.google.common.collect.Sets;
import com.lazy.realtime.common.base.BaseDataStreamApp;
import com.lazy.realtime.common.util.HBaseUtil;
import com.lazy.realtime.common.util.JDBCUtil;
import com.lazy.realtime.common.util.PropertyUtil;
import com.lazy.realtime.dim.function.HBaseSinkFunction;
import com.lazy.realtime.dim.function.HBaseSinkFunction2;
import com.lazy.realtime.dim.pojo.TableProcess;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;
import org.apache.hadoop.hbase.client.Admin;

import java.util.*;
import java.util.stream.Collectors;

/**
 * @Name: Lazy
 * @Date: 2023/12/28 18:54:42
 * @Details:
 */

@Slf4j //自动在类中提供一个名字为 log的 Logger
public class DimApp extends BaseDataStreamApp {
    public static void main(String[] args) {
        new DimApp()
                .start(
                       "dim_app",
                       10001,
                       4,
                        PropertyUtil.getStringValue("TOPIC_ODS_DB"),
                        "Lazy"
                );

    }

    @Override
    protected void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds) {
        //1.ETL 业务数据
        SingleOutputStreamOperator<String> etlDS = etl(ds);

        //etlDS.print();
        /*
           2.
                1)从过滤的gmall库中的数据中，再挑选出维度数据，写出到HBase中
                业务数据中的16张维度表，需要在hbase中创建16张表与之对应.
                    编写HBase的工具类，建库建表

                2)流中混杂了两种数据，一种是维度，一种是事实。
                        应该只处理维度。
                    如何在代码中过滤出维度?
                        通过CDC的方式消费一张配置表，获取要过滤的维度的信息

         */
        SingleOutputStreamOperator<TableProcess> cdcDs = getDimConfig(env);

        //3.根据维度表的配置信息，在hbase中建表
        SingleOutputStreamOperator<TableProcess> cdcDs2 = createHbaseTable(cdcDs);

        //cdcDs2.printToErr();

        //4.connect 维度配置 和 维度数据
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedDs = connect(etlDS, cdcDs2);

        //5.准备写出到HBase。只保留需要写出的字段
        SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> resultDs = dropFileds(connectedDs);

        /*
            DataStreamAPI中并没有提供HBase的连接器。但是FlinkSql中提供了HBase的连接器。
                选择使用FlinkSql，需要：
                        1.把当前的流，分流为16张维度表的流
                        2.创建16张对应的写出到hbase的sink表。
                        3.把16个流分钟转换为16张source表，之后执行 insert into sink表 select * from source表
         */

        //6.执行写出
        resultDs.addSink(new HBaseSinkFunction2());
        resultDs.print();

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> dropFileds(SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connectedDs) {

        return connectedDs
                .map(new MapFunction<Tuple2<JSONObject, TableProcess>, Tuple2<JSONObject, TableProcess>>()
                {
                    @Override
                    public Tuple2<JSONObject, TableProcess> map(Tuple2<JSONObject, TableProcess> value) throws Exception {
                        //取出要过滤的业务数据
                        JSONObject originalData = value.f0;
                        //获取到要保留的字段
                        TableProcess config = value.f1;
                        Set<String> fileds = Arrays.stream(config.getSinkColumns().concat(",").concat("op_type").split(",")).collect(Collectors.toSet());
                        //使用要保留的字段，对原始的业务数据，进行过滤，只留下要保留的字段
                   /* JSONObject result = new JSONObject();
                    Set<Map.Entry<String, Object>> entries = originalData.entrySet();
                    for (Map.Entry<String, Object> entry : entries) {
                        if (fileds.contains(entry.getKey())){
                            result.put(entry.getKey(),entry.getValue());
                        }
                    }*/
                    /*
                        filterKeys(Map<K, V> unfiltered, Predicate<? super K> keyPredicate):
                            Map<K, V> unfiltered: 要过滤的Map
                            Predicate<? super K> 一个key的验证条件。把验证条件返回true的key留下来
                     */
                        value.f0 = new JSONObject(Maps.filterKeys(originalData, fileds::contains));
                        return value;
                    }
                });

    }

    private SingleOutputStreamOperator<Tuple2<JSONObject, TableProcess>> connect(SingleOutputStreamOperator<String> dimDs, SingleOutputStreamOperator<TableProcess> configDs) {

        /*
            设计广播状态中存储的K,V
                K:  维度数据的表名，也就是table_process的 source_table字段
                V： TableProcess
         */
        MapStateDescriptor<String, TableProcess> mapStateDescriptor = new MapStateDescriptor<>("configState", String.class, TableProcess.class);
        //把配置流制作为广播流
        BroadcastStream<TableProcess> broadcastStream = configDs.broadcast(mapStateDescriptor);
        /*
            连接

                存在的问题:
                    一旦维度业务数据到达时，维度的配置信息尚未到达，会导致业务数据无法从广播状态中获取对应的配置信息。
                 解决的思路:
                     业务数据无法从广播状态中获取对应的配置信息，就直接查询Mysql中的gmall2023_config.table_process表，获取对应的配置信息。

                     如果每一条业务数据都向Mysql发起查询请求，Mysql的压力过大。因此可以在process算子的Task启动时，一次性把gmall2023_config.table_process
                     提前查询出来，作为备用。

         */
        return dimDs
                .connect(broadcastStream)
                .process(new BroadcastProcessFunction<String, TableProcess, Tuple2<JSONObject, TableProcess>>()
                {
                    /*
                        RawState:  不便之处在于，你需要自己去备份，恢复状态。
                            Map<String,TableProcess> configMap:
                                key:  source_table。 业务表的名字
                                value:  TableProcess。 配置信息对象
                     */
                    private Map<String,TableProcess> configMap = new HashMap<>();
                    //在生命周期的方法中，不能操作管理状态
                    @Override
                    public void open(Configuration parameters) throws Exception {
                        //查询gmall2023_config.table_process的配置信息，存入集合中
                        List<TableProcess> result = JDBCUtil.queryBeanList("select * from table_process where sink_type = 'DIM' ", TableProcess.class);
                        for (TableProcess tableProcess : result) {
                            configMap.put(tableProcess.getSourceTable(),tableProcess);
                        }
                    }

                    //处理业务数据(事实+维度),写(增，删，改)入hbase的维度表中。通过maxwell采集的数据中的type来控制当前对hbase中维度表的数据进行增，删，改
                    @Override
                    public void processElement(String value, ReadOnlyContext ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {

                        ReadOnlyBroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        JSONObject jsonObject = JSON.parseObject(value);
                        //业务表的名字
                        String table = jsonObject.getString("table");
                    /*
                        取出的v可能为null。
                            情形一：  value当前是事实表的数据，不是维度表的数据
                            情形二：  value当前是维度表的数据，但是广播状态中，还没有收到维度的配置信息
                     */
                        TableProcess v = broadcastState.get(table);

                        if (v == null){
                            v = configMap.get(table);
                            //log.warn(v+" 无法从广播状态中获取....");
                        }

                        //不管是bootstrap-insert还是insert，统一都设置为insert。不做这个操作也可以
                        String type = jsonObject.getString("type").replace("bootstrap-","");
                        //记录当前这条业务数据的操作方式
                        JSONObject data = jsonObject.getJSONObject("data");
                        data.put("op_type",type);

                        if (v != null) {
                            //说明当前数据是你要采集的维度数据。接下来需要再判断，当前记录的操作类型是否和source_type(要采集的类型)一致
                            String sourceType = v.getSourceType();
                            if ("ALL".equals(sourceType)) {
                                out.collect(Tuple2.of(data, v));
                            } else {
                                String[] ops = sourceType.split(",");
                                Set<String> opsets = Arrays.stream(ops).collect(Collectors.toSet());
                                if (opsets.contains(type)) {
                                    out.collect(Tuple2.of(data, v));
                                }
                            }
                        }
                    }

                    //处理配置，控制hbase中维度表的创建，删除，修改等  ，通过TableProcess的op来控制当前对hbase中维度表的增，删，改
                    @Override
                    public void processBroadcastElement(TableProcess value, Context ctx, Collector<Tuple2<JSONObject, TableProcess>> out) throws Exception {
                        //收到一条配置，放入广播状态。 需要随着 table_process这张表同步变化
                        BroadcastState<String, TableProcess> broadcastState = ctx.getBroadcastState(mapStateDescriptor);

                        if ("d".equals(value.getOp())) {
                            //删除配置
                            broadcastState.remove(value.getSourceTable());
                            //保证初始的map和最新的维度配置是同步的
                            configMap.remove(value.getSourceTable());
                        } else {
                            broadcastState.put(value.getSourceTable(), value);
                            configMap.put(value.getSourceTable(), value);
                        }

                    }
                });


    }

    private SingleOutputStreamOperator<TableProcess> createHbaseTable(SingleOutputStreamOperator<TableProcess> cdcDs) {

        return cdcDs
                .map(new RichMapFunction<TableProcess, TableProcess>()
                {
                    private Admin admin;
                    @Override
                    public void open(Configuration parameters) throws Exception {
                    /*
                        创建连接
                            创建的连接不是Connection，而是Admin对象。
                     */
                        admin = HBaseUtil.getAdmin();
                    }

                    @Override
                    public TableProcess map(TableProcess value) throws Exception {

                        String namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
                        //根据当前这条数据的op，对HBase进行对应的操作。
                        if ("d".equals(value.getOp())){
                            //从mysql的table_process中把一张维度表的配置信息删除了，意味着这个维度表退役了，无需再采集它的维度信息
                            HBaseUtil.dropTable(admin,namespace,value.getSinkTable());
                        }else if("u".equals(value.getOp())){
                            //先删，再创建
                            HBaseUtil.dropTable(admin,namespace,value.getSinkTable());
                            HBaseUtil.createTable(admin,namespace,value.getSinkTable(),value.getSinkFamily());
                        }else {
                            //c，r都需要去建表
                            HBaseUtil.createTable(admin,namespace,value.getSinkTable(),value.getSinkFamily());
                        }

                        return value;
                    }

                    @Override
                    public void close() throws Exception {
                        admin.close();
                    }
                });

    }

    private SingleOutputStreamOperator<TableProcess> getDimConfig(StreamExecutionEnvironment env) {

        //读取变化记录，封装为json字符串
        MySqlSource<String> mySqlSource = MySqlSource
                .<String>builder()
                .hostname(PropertyUtil.getStringValue("MYSQL_HOST"))
                .port(PropertyUtil.getIntValue("MYSQL_PORT"))
                .databaseList(PropertyUtil.getStringValue("CONFIG_DATABASE")) // set captured database
                .tableList(PropertyUtil.getStringValue("CONFIG_DATABASE")+"."+PropertyUtil.getStringValue("CONFIG_TABLE")) // set captured table
                .username(PropertyUtil.getStringValue("MYSQL_USER"))
                .password(PropertyUtil.getStringValue("MYSQL_PASSWORD"))
                //默认就是initial。先读快照，再消费binlog
                .startupOptions(StartupOptions.initial())
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        //维度表配置的变更要遵守顺序。这里全程从读取到处理到广播到下游都使用一个并行度
        return env
                .fromSource(mySqlSource, WatermarkStrategy.noWatermarks(), "config").setParallelism(1)
                .filter(str -> str.contains("DIM")).setParallelism(1)
                .map(new MapFunction<String, TableProcess>()
                {
                    @Override
                    public TableProcess map(String jsonStr) throws Exception {

                        JSONObject jsonObject = JSON.parseObject(jsonStr);
                        //配置表中没有op列，但是后续需要采集op列，判断配置信息是更新了还是删除了
                        String op = jsonObject.getString("op");
                        TableProcess t = null;
                        //取其他列
                        if ("d".equals(op)) {
                            //删除，取before
                            t = JSON.parseObject(jsonObject.getString("before"), TableProcess.class);
                        } else {
                            //CUR，取after
                            t = JSON.parseObject(jsonObject.getString("after"), TableProcess.class);
                        }

                        t.setOp(op);
                        return t;
                    }
                }).setParallelism(1);


    }

    /*
        为什么要ETL：
            1.ods_db 中 混合了，所有binlog记录的库的信息。
                   我们只要gmall库的binlog的信息
                   database = gmall
            2.必须有ts，table,type,data
            3.data中有数据
            4.数据必须是json结构
            5.当前数据的操作类型符合要求
     */
    private SingleOutputStreamOperator<String> etl(DataStreamSource<String> ds) {

        //定义type
        HashSet<String> opSets = Sets.newHashSet("insert", "update", "delete","bootstrap-insert");

        return ds
                .filter(new FilterFunction<String>()
                {
                    @Override
                    public boolean filter(String jsonStr) throws Exception {

                        try {
                            JSONObject jsonObject = JSON.parseObject(jsonStr);
                            //正常的json结构
                            //抽取要验证的字段
                            String database = jsonObject.getString("database");
                            String table = jsonObject.getString("table");
                            String type = jsonObject.getString("type");
                            String data = jsonObject.getString("data");
                            String ts = jsonObject.getString("ts");

                            //StringUtils.isNoneBlank( 参数列表 )： 参数列表中任意一个字符串都不是NULL，白字符，''返回true
                            return PropertyUtil.getStringValue("BUSI_DATABASE").equals(database)
                                    &&
                                    StringUtils.isNoneBlank(table, type, data, ts)
                                    &&
                                    opSets.contains(type)
                                    &&
                                    data.length() > 2;

                        } catch (Exception e) {
                            //不是json
                            log.warn(jsonStr + " 是非法的json格式.");
                            return false;
                        }
                    }
                });


    }
}
