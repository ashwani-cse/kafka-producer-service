package com.kafka.streams.advanced.topology;

import com.kafka.streams.advanced.domain.Alphabet;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.*;

import java.time.Duration;

/**
 * @author Ashwani Kumar
 * Created on 26/08/24.
 */
@Slf4j
public class ExploreJoinsOperatorsTopology {
    public static final String ALPHABET = "alphabet_1"; // A => First letter of English Alphabet
    public static final String ALPHABET_ABBREVIATION = "alphabet-abbreviation_1"; // A => Apple

    public static Topology build() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

        //joinKStreamWithKTable(streamsBuilder);
        // joinKStreamWithGlobalKTable(streamsBuilder);
        //joinKTableWithKTable(streamsBuilder);
        joinKStreamWithKStream(streamsBuilder);

        return streamsBuilder.build();
    }

    private static void joinKStreamWithKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> kStream1 = streamsBuilder.stream(ALPHABET_ABBREVIATION,
                Consumed.with(Serdes.String(), Serdes.String()));
        kStream1
                .print(Printed.<String, String>toSysOut().withLabel("abbreviation-stream"));

        KStream<String, String> kStream2 = streamsBuilder.stream(ALPHABET,
                Consumed.with(Serdes.String(), Serdes.String()));
        kStream2
                .print(Printed.<String, String>toSysOut().withLabel("alphabet-stream"));

        //Create valueJoiner to join KStream with KStream
        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;
        JoinWindows fiveSecJoinWindows = JoinWindows.ofTimeDifferenceWithNoGrace(Duration.ofSeconds(5));
        StreamJoined<String, String, String> streamJoinedParams = StreamJoined.
                with(Serdes.String(), Serdes.String(), Serdes.String())
                .withName("alphabet-join")
                .withStoreName("alphabet-join"); // default changelog topic will not create , now store will be created with this name

        KStream<String, Alphabet> joinedKStream = kStream1
                //.join(kStream2, valueJoiner, joinWindows, streamJoined);
                //.leftJoin(kStream2, valueJoiner, fiveSecJoinWindows, streamJoinedParams);
                .outerJoin(kStream2, valueJoiner, fiveSecJoinWindows, streamJoinedParams);

        joinedKStream
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbreviation"));
    }

    private static void joinKTableWithKTable(StreamsBuilder streamsBuilder) {
        KTable<String, String> kTable1 = streamsBuilder
                .table(ALPHABET_ABBREVIATION,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("abbreviation-store"));
        kTable1
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("abbreviation-table"));

        KTable<String, String> kTable2 = streamsBuilder
                .table(ALPHABET,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabet-store"));
        kTable2
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("alphabet-table"));

        //Create valueJoiner to join KTable with KTable
        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KTable<String, Alphabet> joinedKTable = kTable1
                .join(kTable2, valueJoiner);

        joinedKTable
                .toStream()
                .print(Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbreviation"));
    }

    private static void joinKStreamWithGlobalKTable(StreamsBuilder streamsBuilder) {
        KStream<String, String> kStream = streamsBuilder.stream(ALPHABET_ABBREVIATION,
                Consumed.with(Serdes.String(), Serdes.String()));
        kStream
                .print(Printed.<String, String>toSysOut().withLabel("abbreviation-stream"));

        GlobalKTable<String, String> globalKTable = streamsBuilder
                .globalTable(ALPHABET,
                        Consumed.with(Serdes.String(), Serdes.String()));

        //Create valueJoiner to join KStream with GlobalKTable
        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;
        KeyValueMapper<String, String, String> keyValueMapper = (leftKey, rightKey) -> leftKey;

        KStream<String, Alphabet> joinedKStream = kStream
                .join(globalKTable, keyValueMapper, valueJoiner);

        joinedKStream.print(Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbreviation-global"));
    }

    private static void joinKStreamWithKTable(StreamsBuilder streamsBuilder) {
        KStream<String, String> kStream = streamsBuilder.stream(ALPHABET_ABBREVIATION,
                Consumed.with(Serdes.String(), Serdes.String()));
        kStream
                .print(Printed.<String, String>toSysOut().withLabel("abbreviation-stream"));

        KTable<String, String> kTable = streamsBuilder
                .table(ALPHABET,
                        Consumed.with(Serdes.String(), Serdes.String()),
                        Materialized.as("alphabet-store"));
        kTable
                .toStream()
                .print(Printed.<String, String>toSysOut().withLabel("alphabet-table"));

        //Create valueJoiner to join KStream with KTable
        ValueJoiner<String, String, Alphabet> valueJoiner = Alphabet::new;

        KStream<String, Alphabet> joinedKStream = kStream.join(kTable, valueJoiner);

        // [alphabet-with-abbreviation] : A, Alphabet{abbreviation='Apple', description='A is the First letter of English Alphabet'}
        // [alphabet-with-abbreviation] : B, Alphabet{abbreviation='Ball', description='B is the Second letter of English Alphabet'}
        joinedKStream.print(Printed.<String, Alphabet>toSysOut().withLabel("alphabet-with-abbreviation"));
    }

}
