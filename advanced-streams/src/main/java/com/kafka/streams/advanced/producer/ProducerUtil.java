package com.kafka.streams.advanced.producer;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import static java.time.Instant.now;

public class ProducerUtil {
    private static final Logger logger = LoggerFactory.getLogger(ProducerUtil.class.getName());

    private KafkaProducer<String, String> producer;

    public ProducerUtil() {
        producer = new KafkaProducer<>(props());
        logger.info("Kafka Producer started....");
    }

    private Map<String, Object> props() {
        return Map.of(
                ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class,
                ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class // Assuming messages are strings; change if using JsonSerializer
                // ProducerConfig.PARTITIONER_CLASS_CONFIG, RoundRobinPartitioner.class.getName()
        );
    }

    public RecordMetadata publishMessageSync(String topicName, String key, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);
        return getRecordMetadata(producerRecord);
    }

    public RecordMetadata publishMessageSyncWithDelay(String topicName, String key, String message, long delay) {
        long delayMillis = now().plusSeconds(delay).toEpochMilli();
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, 0, delayMillis, key, message);
        return getRecordMetadata(producerRecord);
    }

    private RecordMetadata getRecordMetadata(ProducerRecord<String, String> producerRecord) {
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = producer.send(producerRecord).get();
            logger.info("produced Record : topicName:{} partition:{}, timestamp:{}",
                    recordMetadata.topic(),
                    recordMetadata.partition(),
                    recordMetadata.timestamp());
            Thread.sleep(1000);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return recordMetadata;
    }

    public void closeProducer() {
        // Tell the producer to flush all messages before closing the producer instance.
        // This will send any pending messages to the brokers and block until all messages are sent.
        this.producer.flush();
        this.producer.close();
        logger.info("Kafka Producer closed");
    }
}