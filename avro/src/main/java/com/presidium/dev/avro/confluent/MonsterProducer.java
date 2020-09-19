package com.presidium.dev.avro.confluent;

import com.presidium.dev.BasicMonster;
import io.confluent.kafka.serializers.KafkaAvroSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class MonsterProducer {

    private static final String MONSTER_TOPIC = "monster-topic";
    private final KafkaProducer<String, BasicMonster> kafkaProducer;

    public MonsterProducer() {
        kafkaProducer = new KafkaProducer<>(createProducerProperties());
    }

    public void sendMonster(BasicMonster basicMonster) {
        kafkaProducer.send(new ProducerRecord<>(MONSTER_TOPIC, basicMonster), (recordMetadata, e) -> {
            System.out.println(recordMetadata);
            System.out.println(e);
        });
    }


    private static Properties createProducerProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.setProperty("acks", "all");
        properties.setProperty("retries", "10");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        return properties;
    }
}
