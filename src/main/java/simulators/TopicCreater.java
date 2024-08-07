package simulators;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.NewTopic;

import java.util.Collections;
import java.util.Map;

/**
 * @author Ashwani Kumar
 * Created on 04/08/24.
 */
public class TopicCreater {

    private static AdminClient adminClient;

    static {
        adminClient = AdminClient.create(props());
    }

    public static Map<String, Object> props() {
        return Map.of(
                AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092"
        );
    }

    public static void create(String topicName, int partitions, int replicationFactor) {
        NewTopic topic = new NewTopic(topicName, partitions, (short) replicationFactor);
        CreateTopicsResult topicsResult = adminClient.createTopics(Collections.singletonList(topic));
        topicsResult.all().whenComplete((v, e) -> {
            if (e != null) {
                System.out.println("Error creating topic: " + e.getMessage());
            } else {
                System.out.println("Topic created successfully");
            }
        });
    }
}
