package com.lazy.realtime.common.function;

import com.alibaba.fastjson.JSONObject;
import com.lazy.realtime.common.bean.TableProcess;
import com.lazy.realtime.common.util.AsyncRedisUtil;
import com.lazy.realtime.common.util.HBaseUtil;
import com.lazy.realtime.common.util.JDBCUtil;
import com.lazy.realtime.common.util.PropertyUtil;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisFuture;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.async.RedisAsyncCommands;
import org.apache.flink.api.common.functions.AbstractRichFunction;
import org.apache.flink.configuration.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.AdvancedScanResultConsumer;
import org.apache.hadoop.hbase.client.AsyncTable;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 14:46:59
 * @Details:
 *      抽取操作redis和hbase的公共代码:
 *         1.创建hbase和redis的客户端
 *             redis: Jedis
 *             hbase:  Table
 *         2.提供读写hbase和redis的公共方法
 */
public class DimAsyncOperateBaseFunction extends AbstractRichFunction
{

    //每个Task所使用的函数对象，有唯一的Map，不是共享的
    protected Map<String, AsyncTable<AdvancedScanResultConsumer>> tableMap = new HashMap<>();
    private RedisClient asyncRedisClient;
    private StatefulRedisConnection<String, String> redisConnection;

    @Override
    public void open(Configuration parameters) throws Exception {
        asyncRedisClient = AsyncRedisUtil.getAsyncRedisClient();
        redisConnection = AsyncRedisUtil.getConnection(asyncRedisClient);

        List<TableProcess> result = JDBCUtil.queryBeanList("select * from table_process where sink_type = 'DIM' ", TableProcess.class);
        String namespace = PropertyUtil.getStringValue("HBASE_NAMESPACE");
        for (TableProcess tableProcess : result) {
            tableMap.put(tableProcess.getSourceTable(), HBaseUtil.getAsyncTable(namespace,tableProcess.getSinkTable()));
        }
    }

    @Override
    public void close() throws Exception {
        AsyncRedisUtil.closeConn(redisConnection);
        AsyncRedisUtil.closeClient(asyncRedisClient);
    }

    //读取redis的维度信息
    protected String getStringFromRedisAsync(String table,String id){
        //要操作的命令是一个异步IO的命令
        RedisAsyncCommands<String, String> commands = redisConnection.async();
        //使用支持异步IO的命令对象，发送命令
        RedisFuture<String> future = commands.get(getRediskey(table, id));

        try {
            return future.get();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    //向redis中写String的维度信息
    protected void setStringToRedisAsync(String table,String id,String v){
        //要操作的命令是一个异步IO的命令
        RedisAsyncCommands<String, String> commands = redisConnection.async();
        commands.setex(getRediskey(table,id),PropertyUtil.getIntValue("JEDIS_STRING_TTL"),v);
    }


    /*
        向hbase中读维度，把一行维度数据封装为一个JSONObject
     */
    protected JSONObject getDataFromHBaseAsync(AsyncTable<AdvancedScanResultConsumer> t, String rowkey) {

        JSONObject jsonObject = new JSONObject();
        Get get = new Get(Bytes.toBytes(rowkey));

        CompletableFuture<Result> future = t.get(get);
        Result result = null;
        try {
            result = future.get();
            Cell[] cells = result.rawCells();
            for (Cell cell : cells) {
                //cell代表一列的一个版本。需要列名，列值
                String columnName = Bytes.toString(CellUtil.cloneQualifier(cell));
                String columnValue = Bytes.toString(CellUtil.cloneValue(cell));
                jsonObject.put(columnName,columnValue);
            }

            return jsonObject;
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    protected String getRediskey(String table,String id){
        return table + ":" +id;
    }
}

