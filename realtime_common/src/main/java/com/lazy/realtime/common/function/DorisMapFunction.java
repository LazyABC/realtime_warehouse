package com.lazy.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.PropertyNamingStrategy;
import com.alibaba.fastjson.serializer.SerializeConfig;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;

/**
 * @Name: Lazy
 * @Date: 2024/1/6 14:34:16
 * @Details:  基于 Flink 的 RichMapFunction 的实现，用于将输入的泛型对象 T 转换为 JSON 字符串
 */
public class DorisMapFunction<T> extends RichMapFunction<T, String>
{
    SerializeConfig serializeConfig ;
    @Override
    public void open(Configuration parameters) throws Exception {
        serializeConfig = new SerializeConfig();
        serializeConfig.setPropertyNamingStrategy(PropertyNamingStrategy.SnakeCase);
    }

    @Override
    public String map(T value) throws Exception {
        return  JSON.toJSONString(value,serializeConfig);
    }
}
