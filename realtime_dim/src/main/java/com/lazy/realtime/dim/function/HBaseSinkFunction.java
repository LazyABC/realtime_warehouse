package com.lazy.realtime.dim.function;

import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.common.util.HBaseUtil;
import com.lazy.realtime.common.util.JDBCUtil;
import com.lazy.realtime.common.util.PropertyUtil;
import com.lazy.realtime.dim.pojo.TableProcess;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @Name: Lazy
 * @Date: 2024/1/2 18:16:07
 * @Details:  从数据库中读取配置信息，以确定如何将数据写入HBase。它根据传入的数据和配置支持HBase表上的插入和删除操作
 * 两种API:
 *         新的api:  流.sinkTo(Sink x);
 *
 *         旧的api:  流.addSink(SinkFunction x);  选择，实现比较简单。
 *                     还需要实现生命周期方法，在open()创建连接，在close()中释放链接
 */
@Slf4j
public class HBaseSinkFunction extends RichSinkFunction<Tuple2<JSONObject, TableProcess>> {
    /*
        获得客户端Table.
            open(): 生命周期方法，在Task被创建时，只执行一次。此时数据还没读到。

            只能去查询数据库中的 gmall_config.table_process 来获取要写出到HBase的表名，再根据表名创建出Table对象

        key： source_table 业务表名
        value： sink_table hbase维度表名所对应的Table对象

     */

   private Map<String, Table> tableMap =  new HashMap<>();

    @Override
    public void open(Configuration parameters) throws Exception {
        List<TableProcess> result = JDBCUtil.queryBeanList("select * from table_process where sink_type = 'DIM' ", TableProcess.class);
        //通过配置查询hbase中使用的业务库的名字
        String namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
        for (TableProcess tableProcess : result){
            tableMap.put(tableProcess.getSourceTable(), HBaseUtil.getTable(namespace, tableProcess.getSinkTable()));
        }
        log.warn("tableMap:" + tableMap.toString());
    }

    //对每条业务数据进行写出
    @Override
    public void invoke(Tuple2<JSONObject, TableProcess> value, Context context) throws Exception {
        JSONObject data = value.f0;
        TableProcess config = value.f1;
        //从配置中获取信息
        String table = config.getSourceTable();
        //指原始数据中,哪个字段作为rowkey
        String sinkRowKey = config.getSinkRowKey();
        //根据字段取出rowkey的值
        String rowkey = data.getString(sinkRowKey);
        String sinkFamily = config.getSinkFamily();
        //获取当前数据的操作类型
        String op_type = data.getString("op_type");
        //获取要写出的Table对象
        Table t = tableMap.get(table);
        log.warn("取出:"+ t.toString());
        //执行写出
        if ("delete".equals(op_type)){
            //从维度表中删除这条记录
             /*
               编写写出的方法
                   一个Delete代表对一行的删除操作。
                       花样：
                            删除一列的最新版本
                            删除一列的所有版本
                            删除一列族的所有版本
                            删除一行的所有版本   (需要)
                 */
            t.delete(new Delete(Bytes.toBytes(rowkey)));
        }else {
            //从维度表中写入这条记录
            Put put = putData(rowkey, sinkFamily, data);
            t.put(put);
        }
    }

    @Override
    public void close() throws Exception {
        Set<Map.Entry<String, Table>> entries = tableMap.entrySet();
        for(Map.Entry<String, Table> entry : entries){
               entry.getValue().close();
        }
    }

    /*
        编写写出的方法
            一个Put代表一行的增或改操作
                一个Put中封装多个Cell(一列的一个版本)
     */

    private Put putData(String rowkey , String sinkFamily , JSONObject data){
        Put put = new Put(Bytes.toBytes(rowkey));
        Set<String> fieldName = data.keySet();
        for(String field : fieldName) {
            put.addColumn(
                    Bytes.toBytes(sinkFamily),
                    Bytes.toBytes(field),
                    //业务数据某些字段的值,可能是null
                    Bytes.toBytes(data.getString(field) == null ? "null" : data.getString(field))
                    );
        }
            return put;
        }
}
