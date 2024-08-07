package simulators;

import java.util.Arrays;
import java.util.List;

/**
 * @author Ashwani Kumar
 * Created on 28/07/24.
 */
public class ConsumerRunner {
    public static void main(String[] args) {
        // Define the list of topics you want to subscribe to
        List<String> topics = Arrays.asList("Hi_topic_v5");
        // Create the consumer utility with multiple topics
        String groupId = "consumer-group-v4";
        ConsumerUtil consumerUtil = new ConsumerUtil(topics, groupId);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Detected a shutdown, calling consumer's wakeup method");
            consumerUtil.closeConsumer();
            try {
                Thread main = Thread.currentThread();
                main.join(); // Wait for the main thread to finish in the case of a shutdown hook
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }));

        consumerUtil.consumeMessages();
    }
}
