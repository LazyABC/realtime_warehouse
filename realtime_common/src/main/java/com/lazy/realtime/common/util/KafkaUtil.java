package com.lazy.realtime.common.util;

import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.ProducerConfig;

/**
 * @Name: Lazy
 * @Date: 2023/12/28 18:49:44
 * @Details:
 */
public class KafkaUtil {
    public static KafkaSource<String> getKafkaSource(String topic, String groupId){
        return  KafkaSource
                .<String>builder()
                .setBootstrapServers(PropertyUtil.getStringValue("KAFKA_BROKERS"))
                .setTopics(topic)
                //.setStartingOffsets(OffsetsInitializer.committedOffsets(OffsetResetStrategy.EARLIEST))
                //为了方便调试
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId)
                //开启ck，默认都会把offset提交到kafka一份
                //.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG,"true")
                //.setProperty(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG,"1000")
                //由于使用KafkaSource，Kafka主题中的数据都是采用2PC提交的形式写入的，因此必须设置隔离级别
                .setProperty(ConsumerConfig.ISOLATION_LEVEL_CONFIG,"read_committed")
                .build();
    }

    public static KafkaSink<String> getKafkaSink(String topic){
        return KafkaSink
                .<String>builder()
                .setBootstrapServers(PropertyUtil.getStringValue("KAFKA_BROKERS"))
                .setRecordSerializer(
                        KafkaRecordSerializationSchema
                                .builder()
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .setTopic(topic)
                                .build()
                )
                //必须设置为EOS
                .setDeliveryGuarantee(DeliveryGuarantee.EXACTLY_ONCE)
                //开启了ck，基于2PC提交的事务写出时，可以给每个事务添加一个前缀
                .setTransactionalIdPrefix("lazy-"+ topic)
                .setProperty(ProducerConfig.BATCH_SIZE_CONFIG, "1000")
                .setProperty(ProducerConfig.LINGER_MS_CONFIG, "1000")
                .setProperty(ProducerConfig.TRANSACTION_TIMEOUT_CONFIG,10 * 60000 +"")
                .build();
    }

}
