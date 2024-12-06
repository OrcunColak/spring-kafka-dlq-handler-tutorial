package com.colak.springtutorial.service;

import com.colak.springtutorial.model.Payment;
import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.stereotype.Component;

@Component

@Getter
@Slf4j
public class KafkaConsumer {

    public static final String TOPIC_NAME = "payment-topic";

    @KafkaListener(topics = TOPIC_NAME, groupId = "payment-group")
    public void receive(Payment payment,
                        @Header(KafkaHeaders.RECEIVED_TOPIC) String topic) {
        log.info("Event on topic={}, payload={}", topic, payment);

        if (payment.getId() == 10) {
            // the message will be sent to DQL topic "payment-topic-dlt"
            throw new RuntimeException("my exception");
        }
    }

}
