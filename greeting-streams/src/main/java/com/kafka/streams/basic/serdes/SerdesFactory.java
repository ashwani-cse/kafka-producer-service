package com.kafka.streams.basic.serdes;

import com.kafka.streams.basic.domain.Greeting;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

/**
 * @author Ashwani Kumar
 * Created on 14/08/24.
 */
public class SerdesFactory {

    static public Serde<Greeting> greetingSerde() {
        return new GreetingSerdes();
    }

    static public Serde<Greeting> greetingSerdeGenrics() {
        JsonSerializer<Greeting> jsonSerializer = new JsonSerializer<>();
        JsonDeserializer<Greeting> jsonDeserializer = new JsonDeserializer<>(Greeting.class);

        return Serdes.serdeFrom(jsonSerializer, jsonDeserializer);
    }
}
