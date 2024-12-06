package com.colak.springtutorial.producertest;

import com.colak.springtutorial.model.Payment;
import com.colak.springtutorial.service.KafkaConsumer;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertTrue;

@SpringBootTest
class KafkaProducerTest {

    @Autowired
    private KafkaProducer producer;

    @Test
    void sendMessage() throws InterruptedException {
        Payment payment = new Payment();

        for (int index = 0; index < 100; index++) {
            payment.setId(index);
            boolean result = producer.send(KafkaConsumer.TOPIC_NAME, payment);
            assertTrue(result);
        }
        TimeUnit.SECONDS.sleep(10);
    }
}
