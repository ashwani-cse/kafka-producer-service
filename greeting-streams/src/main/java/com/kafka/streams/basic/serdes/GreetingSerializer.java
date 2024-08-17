package com.kafka.streams.basic.serdes;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.kafka.streams.basic.domain.Greeting;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.serialization.Serializer;

import java.io.IOException;
import java.io.OutputStream;

/**
 * @author Ashwani Kumar
 * Created on 13/08/24.
 */
@Slf4j
public class GreetingSerializer implements Serializer<Greeting> {

    // to perform serialization of Greeting object
    private ObjectMapper objectMapper;

    public GreetingSerializer(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    /**
     * Convert {@code data} into a byte array.
     *
     * @param topic topic associated with data
     * @param data  typed data
     * @return serialized bytes
     */
    @Override
    public byte[] serialize(String topic, Greeting data) {
        try {
            return objectMapper.writeValueAsBytes(data); // serialize Greeting object to byte array
        } catch (JsonProcessingException e) {
            log.error("JsonProcessingException Serializing Greeting : {} ", e.getMessage(), e);
            throw new RuntimeException(e);
        } catch (Exception e) {
            log.error("Exception Serializing Greeting : {} ", e.getMessage(), e);
            throw new RuntimeException(e);
        }
    }
}
