package com.kafka.orders;

import com.kafka.orders.exception.StreamDeserializationExceptionHandler;
import com.kafka.orders.exception.StreamProcessorCustomErrorHandler;
import com.kafka.orders.topology.OrdersTopology;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsException;

import java.util.List;
import java.util.Properties;

/**
 * @author Ashwani Kumar
 * Created on 07/08/24.
 */
@Slf4j
public class OrdersStreamApp {

    private static Properties props() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "orders-app");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");
        properties.put(StreamsConfig.NUM_STREAM_THREADS_CONFIG, 2); // Runtime.getRuntime().availableProcessors());
        // properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG,
        // Serdes.String().getClass().getName());
        // Serdes.StringSerde.class);

        // properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG,
        // Serdes.String().getClass().getName());
        //  Serdes.StringSerde.class);
        properties.put(StreamsConfig.DEFAULT_DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                StreamDeserializationExceptionHandler.class); // helpful in cases where we dont want to stop application  for any deserialization exception

        return properties;
    }

    public static void main(String[] args) {
        List<String> topics = List.of(OrdersTopology.ORDERS_TOPIC,
                OrdersTopology.GENERAL_ORDERS_TOPIC, OrdersTopology.RESTAURANT_ORDERS_TOPIC);
        TopicCreater.createTopics(props(), topics, 1, 1);

        //Topology topology = OrdersTopology.buildTopology(); // Create the topology
        Topology topology = OrdersTopology.exploreStreamErrors();

        KafkaStreams kafkaStreams = new KafkaStreams(topology, props()); // Create the Kafka Streams application
        kafkaStreams.setUncaughtExceptionHandler(new StreamProcessorCustomErrorHandler()); // Log and continue on exceptions

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
