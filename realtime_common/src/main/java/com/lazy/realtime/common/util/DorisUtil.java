package com.lazy.realtime.common.util;


import org.apache.doris.flink.cfg.DorisExecutionOptions;
import org.apache.doris.flink.cfg.DorisOptions;
import org.apache.doris.flink.cfg.DorisReadOptions;
import org.apache.doris.flink.sink.DorisSink;
import org.apache.doris.flink.sink.writer.serializer.SimpleStringSerializer;

/**
 * @Name: Lazy
 * @Date: 2024/1/6 15:14:24
 * @Details: 获取一个用于将字符串数据写入 Doris 数据库的 Sink 对象，并且在创建 Sink 对象时提供了一些配置选项
 */
public class DorisUtil {

    public static DorisSink<String> getDorisSink(String table){
        return new DorisSink<>(
            DorisOptions.builder()
                        .setFenodes(PropertyUtil.getStringValue("DORIS_FE"))
                        .setTableIdentifier(table)
                        .setUsername(PropertyUtil.getStringValue("DOROS_USER"))
                        .setPassword(PropertyUtil.getStringValue("DORIS_PASSWORD"))
                        .build(),
            DorisReadOptions.builder().build(),
                //不对默认参数做文化设置 DorisExecutions.defaults()
            DorisExecutionOptions
                   .builderDefaults()
                   .disable2PC()
                   .build(),
            new SimpleStringSerializer()
        );
    }
}
