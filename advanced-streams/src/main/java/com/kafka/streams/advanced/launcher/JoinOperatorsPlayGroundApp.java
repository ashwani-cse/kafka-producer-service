package com.kafka.streams.advanced.launcher;

import com.kafka.streams.advanced.TopicCreater;
import com.kafka.streams.advanced.topology.ExploreAggregateOperationsTopology;
import com.kafka.streams.advanced.topology.ExploreJoinsOperatorsTopology;
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
 * Created on 26/08/24.
 */
@Slf4j
public class JoinOperatorsPlayGroundApp {
    private static Properties props() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "joins-operator-app");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.COMMIT_INTERVAL_MS_CONFIG, "5000");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
                // Serdes.String().getClass().getName());
                Serdes.StringSerde.class);

        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
                // Serdes.String().getClass().getName());
                Serdes.StringSerde.class);

        return properties;
    }

    public static void main(String[] args) {
        TopicCreater.createTopics(props(), List.of(ExploreJoinsOperatorsTopology.ALPHABET_ABBREVIATION,ExploreJoinsOperatorsTopology.ALPHABET), 1, 1);

        Topology topology = ExploreJoinsOperatorsTopology.build();

        var kafkaStreams = new KafkaStreams(topology, props());

        Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));

        log.info("##### Starting Joins App streams");
        kafkaStreams.start();
    }
}
