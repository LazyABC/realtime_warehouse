package com.lazy.realtime.common.base;

import com.lazy.realtime.common.util.KafkaUtil;
import com.lazy.realtime.common.util.PropertyUtil;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.time.Duration;

/**
 * @Name: Lazy
 * @Date: 2023/12/28 18:41:27
 * @Details:
 */
public abstract class BaseDataStreamApp {
    //提供一个start()方法，方便子类去启动App
    public void start(String jobName,int port,int parallelism,String topic,String groupId){

        Configuration conf = new Configuration();
        conf.setInteger("rest.port", port);
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment(conf);

        env.setParallelism(parallelism);

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

        //从kafka的某个主题中读取数据
        KafkaSource<String> source = KafkaUtil.getKafkaSource(topic, groupId);
        DataStreamSource<String> ds = env.fromSource(source, WatermarkStrategy.noWatermarks(), "source");

        //具体的转换逻辑，应该是抽象，由子类根据不同的需求去实现
        handle(env,ds);

        try {
            //在webUI上，可以显示job的名字
            env.execute(jobName);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    //子类继承后必须实现的
    protected abstract void handle(StreamExecutionEnvironment env, DataStreamSource<String> ds);

}