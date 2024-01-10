package com.lazy.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.common.util.HBaseUtil;
import com.lazy.realtime.common.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.streaming.api.functions.async.AsyncFunction;
import org.apache.flink.streaming.api.functions.async.ResultFuture;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;

import java.util.Collections;
import java.util.concurrent.CompletableFuture;
import java.util.function.Consumer;
import java.util.function.Function;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 14:45:02
 * @Details: look_up join需要提供两个信息:
 *         外键是什么,不是外键的名字id，而是外键的值(id列对应的值)，由调用者传入
 *         关联哪个维度(业务表)
 */
@Slf4j
public abstract class AsyncLookUpJoinFunction<T> extends DimAsyncOperateBaseFunction implements AsyncFunction<T,T>
{
    //关联哪个维度(业务表)
    private final String dimTable;

    //谁调用这个函数，就传入你想操作的id的值
    public abstract String getIdValue(T value);

    protected AsyncLookUpJoinFunction(String dimTable){
        this.dimTable = dimTable;
    }

    /*
        (T input, ResultFuture<T> resultFuture) :
            input: 处理的数据
            resultFuture： 输出

        异步处理输入的每条T，把T的处理结果，封装为ResultFuture即可。
     */
    @Override
    public void asyncInvoke(T value, ResultFuture<T> resultFuture) throws Exception {

        //异步IO的原理是，算子收到的每条数据，都使用一个单独的线程池中的线程，进行处理
        CompletableFuture
                //从缓存中读取维度信息
                .<String>supplyAsync(() -> getStringFromRedisAsync(dimTable,getIdValue(value)))
                //判断返回的String是否为null，为null，需要查询hbase，把查询结果写入缓存
                .thenApplyAsync(new Function<String, JSONObject>()
                {
                    @Override
                    public JSONObject apply(String v) {
                        //模拟一个耗时操作
                        try {
                            Thread.sleep(5000);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                        JSONObject dimData = null;
                        if (v == null){
                            //如果缓存中读不到，访问hbase
                            AsyncTable<AdvancedScanResultConsumer> table = tableMap.get(dimTable);
                            if (table == null){
                                String namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
                                table = HBaseUtil.getAsyncTable(namespace, "dim_"+dimTable);
                                tableMap.put(dimTable,table);
                            }
                            dimData = getDataFromHBaseAsync(table, getIdValue(value));
                            //把hbase读到的数据，写入到缓存，方便后续使用
                            setStringToRedisAsync(dimTable,getIdValue(value),dimData.toJSONString());
                            log.warn("从hbase查询...."+dimTable);
                        }else {
                            dimData = JSON.parseObject(v);
                            log.warn("从redis查询...."+dimTable);
                        }
                        return dimData;
                    }
                })
                .thenAccept(new Consumer<JSONObject>()
                {
                    @Override
                    public void accept(JSONObject dimData) {
                        //读到维度数据后，把想要的字段添加到事实上
                        extractDimData(value,dimData);
                        resultFuture.complete(Collections.singletonList(value));
                    }
                });

    }

    protected abstract void extractDimData(T value, JSONObject dimData) ;

    @Override
    public void timeout(T input, ResultFuture<T> resultFuture) throws Exception {
        AsyncFunction.super.timeout(input, resultFuture);
        //超时时，可以做一些处理
    }
}

