package com.kafka.streams.basic.serdes;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.streams.basic.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Deserializer;

import java.io.IOException;

/**
 * @author Ashwani Kumar
 * Created on 13/08/24.
 */
@Slf4j
public class GreetingDeserializer implements Deserializer<Greeting> {
    private ObjectMapper objectMapper;

    public GreetingDeserializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Deserialize a record value from a byte array into a value or object.
     *
     * @param topic topic associated with the data
     * @param data  serialized bytes; may be null; implementations are recommended to handle null by returning a value or null rather than throwing an exception.
     * @return deserialized typed data; may be null
     */
    @Override
    public Greeting deserialize(String topic, byte[] data) {
        try {
            return objectMapper.readValue(data, Greeting.class);
        } catch (IOException e) {
            log.error("IOException Deserializing Greeting : {} ", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
