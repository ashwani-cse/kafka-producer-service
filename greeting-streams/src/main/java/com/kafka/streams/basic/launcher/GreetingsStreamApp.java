package com.kafka.streams.basic.launcher;

import com.kafka.streams.basic.topology.GreetingsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.StreamsException;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author Ashwani Kumar
 * Created on 07/08/24.
 */
@Slf4j
public class GreetingsStreamApp {

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "greetings-app");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());

        return properties;
    }

    public static void main(String[] args) {
        List<String> topics = List.of(GreetingsTopology.GREETINGS_TOPIC, GreetingsTopology.GREETINGS_UPPERCASE_TOPIC);
        TopicCreater.createTopics(props(), topics, 1, 1);

        Topology topology = GreetingsTopology.buildTopology(); // Create the topology

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props()); // Create the Kafka Streams application

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close)); // Graceful shutdown

        try {

            kafkaStreams.start(); // Start the Kafka Streams application

        } catch (IllegalStateException e) {
            log.error("Error starting the Kafka Streams application: ", e);
        } catch (StreamsException e) {
            log.error("Error in Kafka Streams: ", e);
        }
    }
}
