package com.kafka.streams.basic.topology;

import com.kafka.streams.basic.domain.Greeting;
import com.kafka.streams.basic.serdes.SerdesFactory;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

/**
 * @author Ashwani Kumar
 * Created on 31/08/24.
 */
public class GreetingsTopologyTest {

    TopologyTestDriver testDriver; // No real broker, no real zookeeper, no real kafka cluster required
    TestInputTopic<String, Greeting> inputTopic;
    TestOutputTopic<String, Greeting> outputTopic;

    @BeforeEach
    public void setup() {
        testDriver = new TopologyTestDriver(GreetingsTopology.buildTopology());
        inputTopic = testDriver.createInputTopic(GreetingsTopology.GREETINGS_TOPIC, Serdes.String().serializer(), SerdesFactory.greetingSerdeGenrics().serializer());
        outputTopic = testDriver.createOutputTopic(GreetingsTopology.GREETINGS_UPPERCASE_TOPIC, Serdes.String().deserializer(), SerdesFactory.greetingSerdeGenrics().deserializer());
    }

    /*
     *  To teardown the test driver after each test
     * */
    @AfterEach
    public void tearDown() {
        testDriver.close();
    }

    // Add test cases here
    @Test
    void buildTopology() {
        inputTopic.pipeInput("GM", new Greeting("Hello, Good Morning", LocalDateTime.now()));

        long queueSize = outputTopic.getQueueSize();
        assertEquals(1, queueSize);
        KeyValue<String, Greeting> keyValue = outputTopic.readKeyValue();
        assertEquals("GM", keyValue.key);
        assertEquals("HELLO, GOOD MORNING", keyValue.value.message());
        assertNotNull(keyValue.value.timeStamp());

    }

    @Test
    void buildTopology_multipleInput() {
        KeyValue<String, Greeting> gm = new KeyValue<>("GM", new Greeting("Hello, Good Morning", LocalDateTime.now()));
        KeyValue<String, Greeting> gn = new KeyValue<>("GN", new Greeting("Hello, Good Night", LocalDateTime.now()));
        KeyValue<String, Greeting> ge = new KeyValue<>("GE", new Greeting("Hello, Good Evening", LocalDateTime.now()));

        inputTopic.pipeKeyValueList(List.of(gm, gn, ge));

        long queueSize = outputTopic.getQueueSize();
        assertEquals(3, queueSize);

        List<KeyValue<String, Greeting>> keyValues = outputTopic.readKeyValuesToList(); // safer side create this statement instead of using like
                                                                                        // outputTopic.readKeyValuesToList().get(0) because it will
                                                                                        // throw IndexOutOfBoundException
                                                                                        // if there is no data in the topic queue

        KeyValue<String, Greeting> keyValue1 = keyValues.get(0);
        assertEquals("GM", keyValue1.key);
        assertEquals("HELLO, GOOD MORNING", keyValue1.value.message());
        assertNotNull(keyValue1.value.timeStamp());

        KeyValue<String, Greeting> keyValue2 = keyValues.get(1);
        assertEquals("GN", keyValue2.key);
        assertEquals("HELLO, GOOD NIGHT", keyValue2.value.message());
        assertNotNull(keyValue2.value.timeStamp());

        KeyValue<String, Greeting> keyValue3 = keyValues.get(2);
        assertEquals("GE", keyValue3.key);
        assertEquals("HELLO, GOOD EVENING", keyValue3.value.message());
        assertNotNull(keyValue3.value.timeStamp());

    }
}
