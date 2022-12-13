package com.project.kafka.controller;

import com.project.kafka.dto.PersonDTO;
import com.project.kafka.model.Person;
import com.project.kafka.producer.MessageStringProducer;
import com.project.kafka.producer.PersonProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;

@Slf4j
@RestController
public class KafkaController {

    private final PersonProducer personProducer;
    private final MessageStringProducer messageStringProducer;

    public KafkaController(PersonProducer personProducer,
                           MessageStringProducer messageStringProducer) {
        this.personProducer = personProducer;
        this.messageStringProducer = messageStringProducer;
    }

    @PostMapping("send-message/{message}")
    public ResponseEntity<String> sendMessage(@PathVariable String message){
        log.info("[Producer] Enviando mensagem: {}", message);

        messageStringProducer.sendMessage(message);
        return ResponseEntity.ok("Mensagem enviada:" + message);
    }

    @PostMapping("send-person")
    public ResponseEntity<String> sendPerson(@RequestBody PersonDTO personDTO){
        log.info("[Producer] Enviando pessoa: {}", personDTO);
        Person person = Person.builder().name(personDTO.getName()).age(personDTO.getAge()).build();

        personProducer.sendMessage(person);
        return ResponseEntity.ok("Mensagem enviada: " + personDTO);
    }
}
