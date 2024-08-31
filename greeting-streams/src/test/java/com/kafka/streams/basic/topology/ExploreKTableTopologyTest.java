package com.kafka.streams.basic.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * @author Ashwani Kumar
 * Created on 31/08/24.
 */
public class ExploreKTableTopologyTest {

    TopologyTestDriver testDriver;
    TestInputTopic<String, String> inputTopic;
    TestOutputTopic<String, String> outputTopic;

    void setup() {
        testDriver = new TopologyTestDriver(ExploreKTableTopology.buildTopology());

        inputTopic = testDriver.createInputTopic(ExploreKTableTopology.WORDS,
                Serdes.String().serializer(), Serdes.String().serializer());

        outputTopic = testDriver.createOutputTopic(ExploreKTableTopology.WORDS_OUTPUT,
                Serdes.String().deserializer(), Serdes.String().deserializer());
    }

    @AfterEach
    void tearDown() {
        testDriver.close();
    }

// Limitation: TopologyTestDriver does not emulate the deduplication behavior of a KTable
    @Test
    void buildTopology() {
        // Initialize the TopologyTestDriver and topics
        setup();

        // Pipe input records into the input topic
        inputTopic.pipeInput("A", "Apple");
        inputTopic.pipeInput("A", "Airplane");
        inputTopic.pipeInput("B", "Ball");
        inputTopic.pipeInput("B", "Bat");

        // Get the current size of the output topic queue
        long queueSize = outputTopic.getQueueSize();

        // Assert that the output topic queue contains 4 records
        assertEquals(4, queueSize); // 4 records in the output topic queue (1 record per input record) but in actual KTable, only 2 records will be there
        System.out.println("output values = " + outputTopic.readKeyValuesToList());
        /*
         * Note: In an actual Kafka Streams application using a KTable, the output would typically have only 2 records,
         * one for each key ("A" and "B"), because a KTable retains only the latest value per key.
         * However, in this test using TopologyTestDriver, the output topic will contain all 4 records
         * since TopologyTestDriver does not emulate the deduplication behavior of a KTable.
         * Therefore, the output topic contains all four records and   the queue size in this test will be 4.
         * To fully test KTable behavior, this topology should be run on a real Kafka cluster.
         */
    }


}
