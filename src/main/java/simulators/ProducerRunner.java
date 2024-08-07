package simulators;

import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ashwani Kumar
 * Created on 27/07/24.
 */
public class ProducerRunner {
    private static final Logger logger = LoggerFactory.getLogger(ProducerRunner.class.getName());

    public static void main(String[] args) {
        ProducerUtil producerUtil = new ProducerUtil();

        Runnable producer1 = () -> {
            String producer = Thread.currentThread().getName();
            String topic = "Hi_topic_v5";
            //TopicCreater.create(topic, 3, 1);
           // delay(30000);
            String key = "P1_message";
            key=null;
            for (int i = 0; i < 20; i++) {
                String message = "Hi " + i+ " from "+producer;
                RecordMetadata recordMetadata = producerUtil.publishMessageSync(topic, key, message);
                logger.info("Record Metadata 1: " + recordMetadata.partition());
                delay(6000);
            }
        };

        Runnable producer2 = () -> {
            String producer = Thread.currentThread().getName();
            String topic = "Hello_topic";
            TopicCreater.create(topic, 3, 1);

            String key = "P2_message";
            for (int i = 0; i < 5; i++) {
                String message = "Hello " + i+ " from "+producer;
                RecordMetadata recordMetadata = producerUtil.publishMessageSync(topic, key, message);
                logger.info("Record Metadata 2: " + recordMetadata);
                delay(5000);
            }
        };

        Thread t1 = new Thread(producer1, "Producer 1");
        Thread t2 = new Thread(producer2, "Producer 2");
        t1.start();
       // t2.start();
        try {
            t1.join();
           // t2.join();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
        producerUtil.closeProducer();
    }

    private static void delay(long millis) {
        try {
            Thread.sleep(millis);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
