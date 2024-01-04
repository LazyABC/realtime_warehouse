package com.lazy.realtime.common.base;

import com.lazy.realtime.common.util.PropertyUtil;
import com.lazy.realtime.common.util.SqlUtil;
import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import java.time.Duration;

/**
 * @Name: Lazy
 * @Date: 2024/1/3 11:15:12
 * @Details: 这个框架提供了一些通用的设置，使得子类可以专注于实现业务逻辑，例如从 Kafka 读取数据、进行转换处理，然后将结果写入其他存储系统。
 *             其中，createOdsDb 方法用于创建输入数据表，createDimBaseDic 方法用于创建 HBase 的 Lookup 表
 */
public abstract class BaseSqlApp {
    //提供一个start()方法，方便子类去启动App
    public void start(String jobName,int port,int parallelism){

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        //设置job的名字
        conf.setString("pipeline.name",jobName);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(parallelism);
        //限制最大重启次数，方便调试
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3,2000));

        env.enableCheckpointing(PropertyUtil.getIntValue("CK_INTERVAL"));

        CheckpointConfig checkpointConfig = env.getCheckpointConfig();
        //checkpointConfig.setCheckpointStorage("hdfs://hadoop102:8020/ck/"+jobName);
        checkpointConfig.setCheckpointStorage("file:///e:/tmp/ck/"+jobName);

        //设置ck的超时时间
        checkpointConfig.setCheckpointTimeout(5 * 6000);
        //两次ck之间，必须间隔500ms
        checkpointConfig.setMinPauseBetweenCheckpoints(2000);
        //设置ck的最大的并发数. 非对齐，强制为1
        checkpointConfig.setMaxConcurrentCheckpoints(1);
        //如果开启的是Barrier对齐，那么当60s还没有对齐完成，自动转换为非对齐
        checkpointConfig.setAlignedCheckpointTimeout(Duration.ofSeconds(60));
        //默认情况，Job被cancel了，存储在外部设备的ck数据会自动删除。你可以设置永久持久化。
        checkpointConfig.setExternalizedCheckpointCleanup(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        //一旦ck失败到达10次，此时job就终止
        checkpointConfig.setTolerableCheckpointFailureNumber(10);

        //需要提供一个表的执行环境
        StreamTableEnvironment tableEnvironment = StreamTableEnvironment.create(env);

        //具体的转换逻辑，应该是抽象，由子类根据不同的需求去实现
        handle(tableEnvironment);


    }

    //子类继承后必须实现的
    protected abstract void handle(TableEnvironment env);

    //帮我去创建ods_db这张表。dwd层所有的业务事实都需要从这张表读取数据
    protected void createOdsDb(TableEnvironment env){

        /*
            json格式，列名不能随便定义，顺序可以随意
         */
        String sql = " create table ods_db( " +
                "  `table` STRING ," +
                "  `database` STRING ," +
                "  `type` STRING ," +
                "  `ts` BIGINT ," +
                "  `data` MAP<STRING,STRING> ," +
                "  `old` MAP<STRING,STRING> ," +
                "  `pt` as PROCTIME() " +
                SqlUtil.getKafkaSourceSql(PropertyUtil.getStringValue("TOPIC_ODS_DB"),"ods_db")
                ;

        env.executeSql(sql);
    }

    //loolup join，joinhbase时，只支持rowkey关联！
    protected void createDimBaseDic(TableEnvironment env){
        String sql = " create table dim_dic_code( " +
                "  id STRING ," +
                "  info Row<dic_name STRING,dic_code STRING > ," +
                "  PRIMARY KEY (id) NOT ENFORCED  " +
                " )with( " +
                " 'connector' = 'hbase-2.2',  " +
                "  'zookeeper.quorum' = 'hadoop102:2181' ," +
                "  'table-name' = 'gmall:dim_base_dic' ," +
                "  'lookup.async' = 'true' , " +
                "  'lookup.cache' = 'PARTIAL' , " +
                "  'lookup.partial-cache.max-rows' = '500' , " +
                "  'lookup.partial-cache.expire-after-write' = '1d' , " +
                "  'lookup.partial-cache.cache-missing-key' = 'true'  " +
                ")";

        env.executeSql(sql);
    }
}
