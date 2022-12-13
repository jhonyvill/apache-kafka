package com.project.kafka.producer;

import com.project.kafka.model.Person;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.stereotype.Component;

@Slf4j
@Component
public class PersonProducer {

    private final RoutingKafkaTemplate kafkaTemplate;
    private final String topicName;

    public PersonProducer(RoutingKafkaTemplate kafkaTemplate, @Value("${spring.kafka.topic.person}") String topicName) {
        this.kafkaTemplate = kafkaTemplate;
        this.topicName = topicName;
    }

    public void sendMessage(Person person){
        kafkaTemplate.send(topicName, person).addCallback(
                success -> log.info("Mensagem enviada com sucesso! Topic: {}. Message: {}", topicName, person.toString()),
                failure -> log.error("Falha ao enviar mensagem! Topic: {}. Message: {}", topicName, person.toString())
        );
    }
}