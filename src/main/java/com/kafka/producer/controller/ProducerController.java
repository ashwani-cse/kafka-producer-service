package com.kafka.orders.controller;

import com.kafka.dto.MessagePayload;
import com.kafka.orders.publisher.KafkaMessagePublisher;
import org.springframework.web.bind.annotation.*;

import java.time.Instant;
import java.util.UUID;

/**
 * @author Ashwani Kumar
 * Created on 28/04/24.
 */

@RestController
public class ProducerController {

    private final KafkaMessagePublisher kafkaMessagePublisher;

    public ProducerController(KafkaMessagePublisher kafkaMessagePublisher) {
        this.kafkaMessagePublisher = kafkaMessagePublisher;
    }

    @GetMapping("/publish/{message}")
    public String publishMessage(@PathVariable String message) {
        kafkaMessagePublisher.publishMessage(message);
        return "Message published successfully";
    }

    @PostMapping("/publish")
    public String publishMessage(@RequestBody MessagePayload payload) {
        String id = UUID.randomUUID().toString();
        payload = new MessagePayload(id, payload.message(), Instant.now().toString());
        kafkaMessagePublisher.publishMessage(payload);
        return "Payload published successfully";
    }

}
