package simulators;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.List;
import java.util.Map;

/**
 * @author Ashwani Kumar
 * Created on 27/07/24.
 */
public class ConsumerUtil {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerUtil.class.getName());
    private final List<String> topics;
    private KafkaConsumer<String, String> consumer;

    private Map<String, Object> props(String groupId) {
        return Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092",
                ConsumerConfig.GROUP_ID_CONFIG, groupId,
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" // Or "earliest", "latest" Or "none"
                //ConsumerConfig.PARTITION_ASSIGNMENT_STRATEGY_CONFIG, CooperativeStickyAssignor.class.getName()  // rebalance (load balancing among consumers) strategy
        );
    }

    public ConsumerUtil(List<String> topics, String groupId) {
        Map<String, Object> props = this.props(groupId);
        this.topics = topics;
        this.consumer = new KafkaConsumer<>(props);
        this.consumer.subscribe(topics); // Subscribe to the topics
        logger.info("Kafka Consumer started, subscribed to topics: " + topics);
    }

    public void consumeMessages() {
        try {
            while (true) {
                logger.info("Polling for messages...");
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                records.forEach(record -> {
                    logger.info("Record:- Key: {}, value: {}, topic: {}, partition: {}, offset: {}",
                            record.key(), record.value(), record.topic(), record.partition(), record.offset());
                });
            }
        } catch (WakeupException e) {
            // Shutdown logic
            logger.info("WakeupException caught, closing consumer");
        } catch(Exception e){
            logger.error("Unexpected Exception in consumeMessages : {}  ", e.getMessage(), e);
        }finally {
            consumer.close();
            logger.info("Kafka Consumer closed");
        }
    }

    public void closeConsumer() {
        consumer.wakeup(); // This will stop the poll loop
    }
}
