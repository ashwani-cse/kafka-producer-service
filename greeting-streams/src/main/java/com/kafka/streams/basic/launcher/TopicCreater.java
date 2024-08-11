package com.kafka.streams.basic.launcher;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.streams.StreamsConfig;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

/**
 * @author Ashwani Kumar
 * Created on 04/08/24.
 */
@Slf4j
public class TopicCreater {

    public static void createTopics(Properties props, List<String> topics, int partitions, int replicationFactor) {
        AdminClient adminClient = AdminClient.create(props);
        List<NewTopic> newTopics = topics
                .stream()
                .map(name -> new NewTopic(name, partitions, (short) replicationFactor))
                .collect(Collectors.toList());

        adminClient.createTopics(newTopics).all().whenComplete((v, e) -> {
            if (e != null) {
                log.error("Error creating topics: ", e);
            } else {
                log.info("Topics created successfully");
            }
        });
    }

}
