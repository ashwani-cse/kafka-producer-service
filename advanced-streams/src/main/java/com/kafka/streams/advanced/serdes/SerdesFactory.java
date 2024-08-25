package com.kafka.streams.advanced.serdes;

import com.kafka.streams.advanced.domain.AlphabetWordAggregate;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * @author Ashwani Kumar
 * Created on 14/08/24.
 */
public class SerdesFactory {
    public static Serde<AlphabetWordAggregate> alphabetWordAggregateSerde() {
        JsonSerializer<AlphabetWordAggregate> serializer = new JsonSerializer<>();
        JsonDeserializer<AlphabetWordAggregate> deserializer = new JsonDeserializer<>(AlphabetWordAggregate.class);
        return Serdes.serdeFrom(serializer, deserializer);
    }

}
