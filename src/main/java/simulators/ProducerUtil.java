package simulators;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

/**
 * @author Ashwani Kumar
 * Created on 27/07/24.
 */
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
        RecordMetadata recordMetadata = null;
        try {
            recordMetadata = producer.send(producerRecord).get();
            logger.info("produced Record : topicName:{} partition:{}, timestamp:{}",
                    topicName,
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

    // void publishers -  Not in used in this project
    public void publishMessageAsync(String topicName, String key, String message) {
        ProducerRecord<String, String> producerRecord = new ProducerRecord<>(topicName, key, message);
        producer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                logger.error("Error publishing message: " + exception.getMessage());
            } else {
                logger.info("produced Record : " + producerRecord);
            }
        });

        // Above Callback: this lambda is the implementation of the onCompletion method of the Callback interface
        // The onCompletion method is called when the record has been acknowledged by the broker or an error has occurred.
        // We can implement in java7 style as well
        producer.send(producerRecord, new org.apache.kafka.clients.producer.Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception != null) {
                    logger.error("Error publishing message: " + exception.getMessage());
                } else {
                    logger.info("produced Record : " + producerRecord);
                }
            }
        });
    }

}
