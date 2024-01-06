package com.lazy.realtime.common.util;

/**
 * @Name: Lazy
 * @Date: 2024/1/3 11:16:21
 * @Details: 提供普通的kafka连接器所需要的连接器参数部分的数据
 */
public class SqlUtil {

    //生成配置Kafka源连接器的SQL语句。包括Kafka主题、引导服务器、群组ID、扫描启动模式和数据格式（JSON）等属性
    public static String getKafkaSourceSql(String topic,String gorupId){
        String sql = " )with( " +
                " 'connector' = 'kafka', " +
                " 'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'properties.group.id' = '%s'," +
                "  'scan.startup.mode' = 'earliest-offset', " +
                "  'format' = 'json' " +
                ")";

        return String.format(sql,topic,PropertyUtil.getStringValue("KAFKA_BROKERS"),gorupId);
    }


    //生成配置Kafka接收器连接器的SQL语句。类似于源连接器，包括Kafka主题、引导服务器和数据格式（JSON）等属性
    public static String getKafkaSinkSql(String topic){
        String sql =  " )with( " +
                " 'connector' = 'kafka', " +
                " 'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'format' = 'json' " +
                ")";

        return String.format(sql,topic,PropertyUtil.getStringValue("KAFKA_BROKERS"));
    }


    //生成配置upsert Kafka接收器连接器的SQL语句。包括Kafka主题、引导服务器、键格式（JSON）和值格式（JSON）等属性
    public static String getUpsertKafkaSinkSql(String topic){
        String sql =  " )with( " +
                " 'connector' = 'upsert-kafka', " +
                " 'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "   'key.format' = 'json'," +
                "  'value.format' = 'json' " +
                ")";

        return String.format(sql,topic,PropertyUtil.getStringValue("KAFKA_BROKERS"));
    }


    //生成配置Doris（分布式分析数据库）接收器连接器的SQL语句。包括Doris前端节点、表标识符、用户名、密码、接收器属性（例如格式、缓冲设置）和两阶段提交配置等属性
    public static String getDorisSinkSql(String table){
        String sql =  " )WITH (" +
                "      'connector' = 'doris'," +
                "      'fenodes' = '%s'," +
                "      'table.identifier' = '%s'," +
                "      'username' = '%s'," +
                "      'password' = '%s'," +
                "      'sink.properties.format' = 'json', " +
                "      'sink.properties.read_json_by_line' = 'true', " +
                "      'sink.buffer-count' = '100', " +
                "      'sink.buffer-flush.interval' = '1s', " +
                "      'sink.enable-2pc' = 'false' " +   // 测试阶段可以关闭两阶段提交,方便测试
                ")";

        return String.format(sql,PropertyUtil.getStringValue("DORIS_FE"),table,
                PropertyUtil.getStringValue("DORIS_USER"),
                PropertyUtil.getStringValue("DORIS_PASSWORD"));
    }


    //生成配置HBase源连接器的SQL语句。包括ZooKeeper群集、表名以及异步查找和部分缓存等设置
    public static String getHBaseSourceSql(String table){
        String sql = " )with( " +
                " 'connector' = 'hbase-2.2',  " +
                "  'zookeeper.quorum' = '%s' ," +
                "  'table-name' = '%s' ," +
                "  'lookup.async' = 'true' , " +
                "  'lookup.cache' = 'PARTIAL' , " +
                "  'lookup.partial-cache.max-rows' = '500' , " +
                "  'lookup.partial-cache.expire-after-write' = '1d' , " +
                "  'lookup.partial-cache.cache-missing-key' = 'true'  " +
                ")";

        return String.format(sql,PropertyUtil.getStringValue("HBASE_ZK"),table);
    }
}
