package com.kafka.streams.advanced.producer;

import com.kafka.streams.advanced.topology.ExploreAggregateOperationsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;

/**
 * @author Ashwani Kumar
 * Created on 24/08/24.
 */
@Slf4j
public class AggregateProducer {
    public static void main(String[] args) {
        String topic = ExploreAggregateOperationsTopology.AGGREGATE_WORDS;
        ProducerUtil producerUtil = new ProducerUtil();
      //  String key1 = null; // null key will be skipped by Kafka
        String key1 = "AA";
        String apple = "Apple";
        String alligator = "Alligator";
        String ant = "Ant";

        String key2 = "BB";
        String ball = "Ball";
        String bat = "Bat";

        RecordMetadata recordMetadata = producerUtil.publishMessageSync(topic, key1, apple);
        log.info("Published word with key : {}, ", key1, recordMetadata);
        recordMetadata = producerUtil.publishMessageSync(topic, key1, alligator);
        log.info("Published word with key : {}, ", key1, recordMetadata);
        recordMetadata = producerUtil.publishMessageSync(topic, key1, ant);
        log.info("Published word with key : {}, ", key1, recordMetadata);

        recordMetadata = producerUtil.publishMessageSync(topic, key2, ball);
        log.info("Published word with key : {}, ", key2, recordMetadata);
        recordMetadata = producerUtil.publishMessageSync(topic, key2, bat);
        log.info("Published word with key : {}, ", key2, recordMetadata);
    }
}
