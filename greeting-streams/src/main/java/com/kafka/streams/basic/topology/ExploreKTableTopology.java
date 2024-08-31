package com.kafka.streams.basic.topology;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

/**
 * @author Ashwani Kumar
 * Created on 24/08/24.
 */
@Slf4j
public class ExploreKTableTopology {
    public static String WORDS = "words";
    public static String WORDS_OUTPUT = "words-output";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KTable<String, String> stringKTable = streamsBuilder
                .table(WORDS,
                        //  GlobalKTable<String, String> stringKTable = streamsBuilder
                        //         .globalTable(WORDS,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("stringKTable"));

        stringKTable.filter((key, value) -> value.length() > 1)
                .toStream()
                .to(WORDS_OUTPUT, Produced.with(Serdes.String(), Serdes.String()));
               // .peek((key, value) -> log.info("key : {}, value : {}", key, value))
               //.print(Printed.<String, String>toSysOut().withLabel("filteredKTable"));
        return streamsBuilder.build();
    }
}
