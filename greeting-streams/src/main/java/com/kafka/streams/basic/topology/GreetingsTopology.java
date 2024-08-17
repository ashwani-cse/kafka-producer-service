package com.kafka.streams.basic.topology;

import com.kafka.streams.basic.domain.Greeting;
import com.kafka.streams.basic.serdes.SerdesFactory;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Printed;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * @author Ashwani Kumar
 * Created on 07/08/24.
 */
@Slf4j
public class GreetingsTopology {

    public static String GREETINGS_TOPIC = "greetings_1";
    public static String GREETINGS_UPPERCASE_TOPIC = "greetings-uppercase_1";
    public static String GREETINGS_SPANIISH_TOPIC = "greetings-spanish_1";

    /**
     * Build the topology for the Kafka Streams application which represents the stream processing logic.
     */
    public static Topology buildTopology() {
        StreamsBuilder streamsBuilder = new StreamsBuilder();

       // KStream<String, String> mergedKStream = getStringGreetingKStream(streamsBuilder);
        KStream<String, Greeting> mergedKStream = getCustomGreetingKStream(streamsBuilder);

        mergedKStream.print(Printed.<String, Greeting>toSysOut().withLabel("mergedKStream"));

        KStream<String, Greeting> modifiedKStream = mergedKStream
                //.filter((key, value) -> value.length() > 5)
                //.filterNot((key, value) -> value.length() > 5)
                // .peek((key, value)-> {
                /* key = key.substring(0, 3); // any modification to key & value will not be reflected in the stream
                 value = null;*/
                // log.info("after filter- key : {}, value : {}", key, value);
                // })
                .mapValues((readOnlyKey, value) -> new Greeting(value.message().toUpperCase(), value.timeStamp()));
        // .peek((key, value)-> log.info("after mapValues- key : {}, value : {}", key, value));
        //.map((key, value) -> KeyValue.pair(key.toUpperCase(), value.toUpperCase()));
/*                .flatMap((key, value) -> {
                    List<String> list = Arrays.asList(value.split(""));
                    return list
                            .stream()
                            .map(val->KeyValue.pair(key, val.toUpperCase()))
                            .collect(Collectors.toList());
                });*/
/*        .flatMapValues((key, value)->{
            List<String> list = Arrays.asList(value.split(""));
            return list
                    .stream()
                    .map(val->val.toUpperCase())
                    .collect(Collectors.toList());
        });*/

       // modifiedKStream.print(Printed.<String, String>toSysOut().withLabel("modifiedKStream"));
        modifiedKStream.print(Printed.<String, Greeting>toSysOut().withLabel("modifiedKStream"));

        modifiedKStream
                .to(GREETINGS_UPPERCASE_TOPIC
                        //, Produced.with(Serdes.String(), SerdesFactory.greetingSerde())
                        , Produced.with(Serdes.String(), SerdesFactory.greetingSerdeGenrics())
                );
        return streamsBuilder.build();
    }

    private static KStream<String, Greeting> getCustomGreetingKStream(StreamsBuilder streamsBuilder) {
        KStream<String, Greeting> kStream1 = streamsBuilder
                .stream(GREETINGS_TOPIC
                       // , Consumed.with(Serdes.String(), SerdesFactory.greetingSerde())
                        , Consumed.with(Serdes.String(), SerdesFactory.greetingSerdeGenrics())
                );

        KStream<String, Greeting> kStream2 = streamsBuilder
                .stream(GREETINGS_SPANIISH_TOPIC
                       // , Consumed.with(Serdes.String(), SerdesFactory.greetingSerde())
                        , Consumed.with(Serdes.String(), SerdesFactory.greetingSerdeGenrics())
                );

        KStream<String, Greeting> mergedKStream = kStream1.merge(kStream2);

        return mergedKStream;
    }

    private static KStream<String, String> getStringGreetingKStream(StreamsBuilder streamsBuilder) {
        KStream<String, String> kStream1 = streamsBuilder
                .stream(GREETINGS_TOPIC
                        , Consumed.with(Serdes.String(), Serdes.String())
                );

        KStream<String, String> kStream2 = streamsBuilder
                .stream(GREETINGS_SPANIISH_TOPIC
                        , Consumed.with(Serdes.String(), Serdes.String())
                );

        KStream<String, String> mergedKStream = kStream1.merge(kStream2);

        mergedKStream.print(Printed.<String, String>toSysOut().withLabel("mergedStream"));
        return mergedKStream;
    }

}
