package com.kafka.streams.basic.launcher;

import com.kafka.streams.basic.TopicCreater;
import com.kafka.streams.basic.topology.ExploreKTableTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

/**
 * @author Ashwani Kumar
 * Created on 24/08/24.
 */
@Slf4j
public class KTableStreamApp {

    private static Properties props(){
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "ktable-app");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        return properties;
    }
    public static void main(String[] args) {
        Topology topology = ExploreKTableTopology.buildTopology();
        TopicCreater.createTopics(props(), List.of(ExploreKTableTopology.WORDS), 1, 1);
        var kafkaStreams = new KafkaStreams(topology, props());
        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
        log.info("##### Starting KTable streams");
        kafkaStreams.start();
    }
}
