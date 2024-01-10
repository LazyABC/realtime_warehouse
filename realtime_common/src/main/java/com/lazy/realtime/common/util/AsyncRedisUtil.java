package com.lazy.realtime.common.util;

import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;

/**
 * @Name: Lazy
 * @Date: 2024/1/10 14:22:14
 * @Details:  有异步IO功能的Redis客户端
 */
public class AsyncRedisUtil {
    //提供客户端
    public static RedisClient getAsyncRedisClient(){
        String url = "redis://%s:%s/%s";
        return RedisClient.create(
                String.format(url,
                        PropertyUtil.getStringValue("JEDIS_POOL_HOST"),
                        PropertyUtil.getStringValue("JEDIS_POOL_PORT"),
                        PropertyUtil.getStringValue("JEDIS_DB_ID")

                ));
    }

    //使用客户端连接上redis服务端
    public static StatefulRedisConnection<String, String> getConnection(RedisClient client){
        return client.connect();
    }

    //关闭连接
    public static void closeConn(StatefulRedisConnection<String, String> conn){
        if (conn != null){
            conn.close();
        }
    }

    //释放客户端
    public static void closeClient(RedisClient client){
        if (client != null){
            client.close();
        }
    }


}
