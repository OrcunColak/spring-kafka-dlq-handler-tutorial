package com.colak.springtutorial.config;

import com.colak.springtutorial.model.Payment;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.listener.ErrorHandlingUtils;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;

@Configuration
@EnableKafka

@RequiredArgsConstructor
@Slf4j
public class KafkaConsumerConfig {

    private final KafkaTemplate<String, Payment> kafkaTemplate;

    protected Map<String, Object> consumerConfig() {
        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, false);

        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        return config;
    }

    @Bean
    public ConsumerFactory<String, Payment> consumerFactory() {
        JsonDeserializer<Payment> valueDeserializer = new JsonDeserializer<>();
        valueDeserializer.addTrustedPackages("*");

        Deserializer<String> stringDeserializer = new StringDeserializer();
        return new DefaultKafkaConsumerFactory<>(consumerConfig(), stringDeserializer, valueDeserializer);
    }

    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Payment> kafkaListenerContainerFactory() {
        ConcurrentKafkaListenerContainerFactory<String, Payment> kafkaListenerContainerFactory = new ConcurrentKafkaListenerContainerFactory<>();
        kafkaListenerContainerFactory.setConsumerFactory(consumerFactory());
        kafkaListenerContainerFactory.setConcurrency(2);
        kafkaListenerContainerFactory.setCommonErrorHandler(defaultErrorHandler()); // uncomment to enable blocking retries.
        kafkaListenerContainerFactory.getContainerProperties().setAckMode(ContainerProperties.AckMode.RECORD);
        return kafkaListenerContainerFactory;
    }

    // spring Kafka default error handler
    @Bean
    public DefaultErrorHandler defaultErrorHandler() {
        // initializing  with recoverer and backoff
        var defaultErrorHandler = new DefaultErrorHandler(deadLetterPublishingRecoverer(), new FixedBackOff(1000L, 2L));
        defaultErrorHandler.setLogLevel(KafkaException.Level.DEBUG);
        // deh.addNotRetryableExceptions(NonRetryableException.class);
        defaultErrorHandler.setAckAfterHandle(true);
        return defaultErrorHandler;
    }

    @Bean
    public DeadLetterPublishingRecoverer deadLetterPublishingRecoverer() {
        return new DeadLetterPublishingRecoverer(
                kafkaTemplate,
                (ConsumerRecord<?, ?> record, Exception exception) -> {

                    // exception is : "org.springframework.kafka.listener.ListenerExecutionFailedException"
                    // unwrapped exception is : "java.lang.RuntimeException: my exception"
                    var ex = ErrorHandlingUtils.unwrapIfNeeded(exception);

                    log.error("exception", exception);
                    log.error("unwrapped exception", ex);

                    String deadLetterTopic = record.topic() + ".DLT";
                    return new TopicPartition(deadLetterTopic, record.partition());
                });
    }
}
