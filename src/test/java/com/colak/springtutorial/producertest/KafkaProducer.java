package com.colak.springtutorial.producertest;

import com.colak.springtutorial.model.Payment;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component

@Getter
@Slf4j
@RequiredArgsConstructor
public class KafkaProducer {

    private final KafkaTemplate<String, Payment> kafkaTemplate;

    public boolean send(String topic, Payment payment) {
        log.info("sending payment='{}' to topic='{}'", payment, topic);

        boolean result = true;
        try {
            kafkaTemplate.send(topic, payment).get();
        } catch (Exception exception) {
            result = false;
        }
        return result;
    }
}
