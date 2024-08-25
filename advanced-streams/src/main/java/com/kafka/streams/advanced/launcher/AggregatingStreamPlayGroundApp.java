package com.kafka.streams.advanced.launcher;

import com.kafka.streams.advanced.TopicCreater;
import com.kafka.streams.advanced.topology.ExploreAggregateOperationsTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

import java.util.List;
import java.util.Properties;

/**
 * @author Ashwani Kumar
 * Created on 25/08/24.
 */
@Slf4j
public class AggregatingStreamPlayGroundApp {
    private static Properties props() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "aggregate-record-app");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                // Serdes.String().getClass().getName());
                Serdes.StringSerde.class);

        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                // Serdes.String().getClass().getName());
                Serdes.StringSerde.class);

        return properties;
    }

    public static void main(String[] args) {
        TopicCreater.createTopics(props(), List.of(ExploreAggregateOperationsTopology.AGGREGATE_WORDS), 1, 1);

        Topology topology = ExploreAggregateOperationsTopology.buildTopology();

        var kafkaStreams = new KafkaStreams(topology, props());

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        log.info("##### Starting Aggregating streams");
        kafkaStreams.start();
    }
}
