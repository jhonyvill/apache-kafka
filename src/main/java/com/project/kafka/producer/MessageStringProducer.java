package com.project.kafka.producer;

import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class MessageStringProducer {

    private final RoutingKafkaTemplate kafkaTemplate;
    private final String topicName;

    public MessageStringProducer(RoutingKafkaTemplate kafkaTemplate, @Value("${spring.kafka.topic.message}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void sendMessage(String message){
        kafkaTemplate.send(topicName, message).addCallback(
                success -> log.info("Mensagem enviada com sucesso!"),
                failure -> log.error("Falha ao enviar mensagem!")
        );
    }
}
