package com.lazy.realtime.common.function;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.common.util.HBaseUtil;
import com.lazy.realtime.common.util.PropertyUtil;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.hadoop.hbase.client.Table;


/**
 * @Name: Lazy
 * @Date: 2024/1/9 15:46:08
 * @Details:
 *  look_up join需要提供两个信息:
 *         外键是什么,不是外键的名字id，而是外键的值(id列对应的值)，由调用者传入
 *         关联哪个维度(业务表)
 */
@Slf4j
public abstract class LookUpJoinFunction<T> extends DimOperateBaseFunction implements MapFunction<T,T> {
    //关联哪个维度(业务表)
    private final String dimTable;


    //谁调用这个函数，就传入你想操作的id的值
    public abstract String getIdValue(T value);

    protected LookUpJoinFunction(String dimTable) {
        this.dimTable = dimTable;
    }

    //value: 获取的一条事实数据
    @Override
    public T map(T value) throws Exception {

        JSONObject dimData = null;
        String k = getIdValue(value);
        /*
            从缓存中读取维度信息
                redis中查询value为String的值，如果key不存在，返回值为null
                redis中查询value为Set的值，如果key不存在，返回值不为null，返回空集合[]
         */
        String v = getStringFromRedis(dimTable,k );

        if (v == null){
            //如果缓存中读不到，访问hbase
            Table table = tableMap.get(dimTable);
            if (table == null){
                String namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
                table = HBaseUtil.getTable(namespace, "dim_"+dimTable);
                tableMap.put(dimTable,table);
            }
            dimData = getDataFromHBase(table, k);
            //把hbase读到的数据，写入到缓存，方便后续使用
            setStringToRedis(dimTable,k,dimData.toJSONString());
            log.warn("从hbase查询....");
        }else {
            dimData = JSON.parseObject(v);
            log.warn("从redis查询....");
        }
        //读到维度数据后，把想要的字段添加到事实上
        extractDimData(value,dimData);

        return value;
    }

    protected abstract void extractDimData(T value, JSONObject dimData) ;
}
