package com.project.kafka.config;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.support.GenericApplicationContext;
import org.springframework.kafka.config.TopicBuilder;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.kafka.core.RoutingKafkaTemplate;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Pattern;

@Configuration
public class ProducerKafkaConfig {

    private KafkaProperties kafkaProperties;

    public ProducerKafkaConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    //Configuração para permitir a criação de tópicos via código
    @Bean
    public KafkaAdmin kafkaAdmin(){
        HashMap<String, Object> props = new HashMap<>();
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        return new KafkaAdmin(props);
    }

    //Criação de tópicos via código
    @Bean
    public KafkaAdmin.NewTopics newTopics(){
        return new KafkaAdmin.NewTopics(
                TopicBuilder.name("${spring.kafka.topic.message}").partitions(2).replicas(1).build(),
                TopicBuilder.name("${spring.kafka.topic.person}").partitions(2).replicas(1).build());
    }

    //Configuração para serialização de mensagens Sring.
    @Bean
    public ProducerFactory<Object, Object> producerFactoryString(){
        HashMap<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        return new DefaultKafkaProducerFactory<>(props);
    }

    /*@Bean
    public KafkaTemplate<Object, Object> kafkaTemplate(){
        return new KafkaTemplate<>(producerFactoryString());
    }*/

    //Configuração para serialização de mensagens Json.
    public ProducerFactory producerFactoryJson(){
        HashMap<String, Object> props = new HashMap<>();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaProperties.getBootstrapServers());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class);
        return new DefaultKafkaProducerFactory<>(props, new StringSerializer(), new JsonSerializer<>());
    }

    //Configuração para criar uma lista de templates
    @Bean
    public RoutingKafkaTemplate routingKafkaTemplate(GenericApplicationContext context, ProducerFactory<Object, Object> producerFactory){
        ProducerFactory<Object, Object> producerFactoryJson = producerFactoryJson();
        context.registerBean(DefaultKafkaProducerFactory.class, "producerFactoryJson", producerFactoryJson);

        Map<Pattern, ProducerFactory<Object, Object>> map = new LinkedHashMap<>();
        map.put(Pattern.compile("topic.+"), producerFactoryString());
        map.put(Pattern.compile(".+-topic"), producerFactoryJson);
        return new RoutingKafkaTemplate(map);
    }
}
