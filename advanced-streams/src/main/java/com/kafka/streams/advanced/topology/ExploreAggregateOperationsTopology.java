package com.kafka.streams.advanced.topology;

import com.kafka.streams.advanced.domain.AlphabetWordAggregate;
import com.kafka.streams.advanced.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;
import org.apache.kafka.streams.state.KeyValueStore;

/**
 * @author Ashwani Kumar
 * Created on 24/08/24.
 */
@Slf4j
public class ExploreAggregateOperationsTopology {
    public static String AGGREGATE_WORDS = "aggregate";

    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> aggregateStream = streamsBuilder.stream(AGGREGATE_WORDS,
                Consumed.with(Serdes.String(), Serdes.String()));

        aggregateStream.print(Printed.<String, String>toSysOut().withLabel("aggregate"));

        KGroupedStream<String, String> groupedStream = aggregateStream
                // groupByKey uses published key for grouping
                .groupByKey(Grouped.with(Serdes.String(), Serdes.String()));
        // If we have not published key or we want to group by value or our custom key then we can use groupBy
        //.groupBy((key, value) -> value,
        //      Grouped.with(Serdes.String(), Serdes.String())); // Apple: 3, Alligator: 9, Ant: 3, Ball: 4, Bat: 3 like this

        //exploreCount(groupedStream);
        //exploreReduce(groupedStream);
        //exploreAggregate(groupedStream);
        exploreCustomAggregate(groupedStream);

        return streamsBuilder.build();
    }

    private static void exploreCount(KGroupedStream<String, String> groupedStream) {
        KTable<String, Long> alphabetCountKTable = groupedStream
                //.count(Named.as("count-per-alphabet")); // Kafka Streams will automatically create a default state store for this counting operation, but you won't have direct control over the specifics of that state store (like its name, serdes, or other configurations).
                .count(Named.as("count-per-alphabet"),
                        Materialized.as("count-per-alphabet-store"));
        alphabetCountKTable
                .toStream()
                .print(Printed.<String, Long>toSysOut().withLabel("words-count-per-alphabet"));
    }

    private static void exploreReduce(KGroupedStream<String, String> groupedStream) {
        KTable<String, String> reducedStream = groupedStream
                .reduce((value1, value2) -> {
                    log.info("value1 : {}, value2 : {}", value1, value2);
                    return value1.toUpperCase() + "-" + value2.toUpperCase();
                }, // we can use materialized to store for custom state store name
                        Materialized.<String, String, KeyValueStore<Bytes, byte[]>>as("reduce-words-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(Serdes.String()));

        reducedStream
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("words-reduce-per-alphabet"));
    }

    //aggregate is similar to reduce but it is used when we want to return different type than input type
    private static void exploreAggregate(KGroupedStream<String, String> groupedStream) {
        //create initializer
        Initializer<String> initializer = () -> ""; // initial value of aggregate count of values for a key
        //create Aggregator
        Aggregator<String, String, String> aggregator = (key, value, aggregate) -> {
            log.info("key : {}, value : {}, aggregate : {}", key, value, aggregate);
            return aggregate + value.length(); // a string with the count of each value appended ,
            // like Apple: 5, Alligator: 9, Ant: 3, then output will be "5"+"9"+"3" = 593
        };
        KTable<String, String> aggregatedStream = groupedStream
                .aggregate(initializer, aggregator);

        aggregatedStream
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("words-aggregate-per-alphabet"));
    }

    private static void exploreCustomAggregate(KGroupedStream<String, String> groupedStream) {
        Initializer<AlphabetWordAggregate> initializer = AlphabetWordAggregate::new;
        Aggregator<String, String, AlphabetWordAggregate> aggregator = (key, value, aggregate) -> {
            log.info("key : {}, value : {}, aggregate : {}", key, value, aggregate);
            return aggregate.updateNewEvents(key, value);
        };

        KTable<String, AlphabetWordAggregate> aggregatedStream = groupedStream
                .aggregate(initializer,
                        aggregator,
                        Materialized.<String, AlphabetWordAggregate, KeyValueStore<Bytes, byte[]>>as("custom-aggregate-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(SerdesFactory.alphabetWordAggregateSerde()));


        aggregatedStream
                .toStream()
                .print(Printed.<String, AlphabetWordAggregate>toSysOut().withLabel("words-custom-aggregate-per-alphabet"));
    }
}
