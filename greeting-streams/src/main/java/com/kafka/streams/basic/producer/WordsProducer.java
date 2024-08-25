package com.kafka.streams.basic.producer;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * @author Ashwani Kumar
 * Created on 24/08/24.
 */
@Slf4j
public class WordsProducer {
    public static void main(String[] args) {
        ProducerUtil producerUtil = new ProducerUtil();
      //  String key1 = null; // null key will be skipped by Kafka
        String key1 = "A";
        String apple = "Apple";
        String alligator = "Alligator";
        String ant = "Ant";

        String key2 = "B";
        String ball = "Ball";
        String bat = "Bat";
        String banana = "Banana";

        RecordMetadata recordMetadata = producerUtil.publishMessageSync("words", key1, apple);
        log.info("Published word with key : {}, ", key1, recordMetadata);
        recordMetadata = producerUtil.publishMessageSync("words", key1, alligator);
        log.info("Published word with key : {}, ", key1, recordMetadata);
        recordMetadata = producerUtil.publishMessageSync("words", key1, ant);
        log.info("Published word with key : {}, ", key1, recordMetadata);

        recordMetadata = producerUtil.publishMessageSync("words", key2, ball);
        log.info("Published word with key : {}, ", key2, recordMetadata);
        recordMetadata = producerUtil.publishMessageSync("words", key2, bat);
        log.info("Published word with key : {}, ", key2, recordMetadata);
        recordMetadata = producerUtil.publishMessageSync("words", key2, banana);
        log.info("Published word with key : {}, ", key2, recordMetadata);
    }
}
