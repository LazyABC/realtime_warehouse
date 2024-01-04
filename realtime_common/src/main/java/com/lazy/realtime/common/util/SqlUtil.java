package com.lazy.realtime.common.util;

/**
 * @Name: Lazy
 * @Date: 2024/1/3 11:16:21
 * @Details: 提供了两个静态方法，用于生成 Kafka 连接器的 SQL 语句，方便在 Flink 程序中配置 Kafka 相关的参数
 */
public class SqlUtil {
    //提供普通的kafka连接器所需要的连接器参数部分的数据
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

    public static String getKafkaSinkSql(String topic){
        String sql =  " )with( " +
                " 'connector' = 'kafka', " +
                " 'topic' = '%s'," +
                "  'properties.bootstrap.servers' = '%s'," +
                "  'format' = 'json' " +
                ")";

        return String.format(sql,topic,PropertyUtil.getStringValue("KAFKA_BROKERS"));
    }

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
}
