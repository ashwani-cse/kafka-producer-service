package com.kafka.orders.publisher;

import com.kafka.dto.MessagePayload;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

/**
 * @author Ashwani Kumar
 * Created on 28/04/24.
 */

@Slf4j
@Component
public class KafkaMessagePublisher {

    private static final String TOPIC_WITH_PARTITIONS_3 = "topic-with-partitions-3";
    private static final String MESSAGE_PAYLOAD_TOPIC = "message-payload-topic";


    private final KafkaTemplate<String, Object> kafkaTemplate;

    public KafkaMessagePublisher(KafkaTemplate<String, Object> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    public void publishMessage(String message) {
        for (int i = 94; i < 10000; i++) {
            CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(TOPIC_WITH_PARTITIONS_3, message + " " + i);
            future.whenComplete((result, ex) -> {
                if (ex == null) {
                    log.info("Message published: {}, offset: {}", message, result.getRecordMetadata().offset());
                } else {
                    log.error("Error publishing message: {}", message);
                }
            });
        }
    }

    public void publishMessage(MessagePayload payload) {
        //1.  Without giving partition number
        //CompletableFuture<SendResult<String, Object>> future = kafkaTemplate.send(MESSAGE_PAYLOAD_TOPIC, payload.id(), payload);

        //2. we can give partition number as well
        CompletableFuture<SendResult<String, Object>> future =  kafkaTemplate.send(MESSAGE_PAYLOAD_TOPIC, 0, payload.id(), payload);

        future.whenComplete((result, ex) -> {
            if (ex == null) {
                log.info("Payload published: {}, offset: {}", payload.toString(), result.getRecordMetadata().offset());
            } else {
                log.error("Error publishing payload: {}", payload.toString());
            }
        });
    }
}
