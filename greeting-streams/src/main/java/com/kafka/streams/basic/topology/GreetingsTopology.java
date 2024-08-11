package com.kafka.streams.basic.topology;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

/**
 * @author Ashwani Kumar
 * Created on 07/08/24.
 */
public class GreetingsTopology {

    public static String GREETINGS_TOPIC = "greetings";
    public static String GREETINGS_UPPERCASE_TOPIC = "greetings-uppercase";

    /**
     * Build the topology for the Kafka Streams application which represents the stream processing logic.
     */
    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> kStream = streamsBuilder
                .stream(GREETINGS_TOPIC,
                        Consumed.with(Serdes.String(), Serdes.String()));

        kStream.print(Printed.<String, String>toSysOut().withLabel("greetingsStream"));

        KStream<String, String> modifiedKStream = kStream
                //.filter((key, value) -> value.length() > 5)
                //.filterNot((key, value) -> value.length() > 5)
                //.mapValues((readOnlyKey, value) -> value.toUpperCase());
                .map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));

        modifiedKStream.print(Printed.<String, String>toSysOut().withLabel("modifiedKStream"));

        modifiedKStream.to(GREETINGS_UPPERCASE_TOPIC,
                Produced.with(Serdes.String(), Serdes.String()));

        return streamsBuilder.build();
    }

}
