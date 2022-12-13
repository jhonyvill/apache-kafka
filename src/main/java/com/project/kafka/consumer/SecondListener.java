package com.project.kafka.consumer;

import com.project.kafka.model.Person;
import com.project.kafka.repository.PersonRepository;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.listener.adapter.ConsumerRecordMetadata;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;


@Slf4j
@Component
public class SecondListener {

    PersonRepository personRepository;

    public SecondListener(PersonRepository personRepository) {
        this.personRepository = personRepository;
    }

    @KafkaListener(topics = "${spring.kafka.topic.message}", groupId = "message-group", containerFactory = "kafkaListener")
    public void listenerString(String message, ConsumerRecordMetadata metadata){
        log.info("[Consumer - Second Listener] Tópico {}. Lendo Mensagem: {}", metadata.topic(), message);
    }

    @KafkaListener(topics = "${spring.kafka.topic.person}", groupId = "person-group", containerFactory = "jsonKafkaListener")
    public void listenerJson(Person person,
                             ConsumerRecordMetadata metadata,
                             Acknowledgment ack){
        if (person.getName().isBlank()) {
            log.error("[Consumer - Second Listener] Erro ao ler mensagem.");
            throw new IllegalArgumentException("Valor inválido para mensagem.");
        }

        log.info("[Consumer - Second Listener] Tópico: {}. Lendo Mensagem: {}.", metadata.topic(), person);
        Person savedPerson = personRepository.save(person);

        log.info("[Consumer - Second Listener] Pessoa salva: {}", savedPerson);
        ack.acknowledge();
    }

    @KafkaListener(topics = "${spring.kafka.topic.person.dlt}", groupId = "person-group", containerFactory = "jsonKafkaListener")
    public void listenerDLT(Person person, Acknowledgment ack){
        log.info("[Consumer - DLT] Lendo Mensagem: {}", person);
        ack.acknowledge();
    }

}
