package com.presidium.dev.avro.confluent;

import com.presidium.dev.BasicMonster;
import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class MonsterConsumer {

    private static final String MONSTER_TOPIC = "monster-topic";
    private final KafkaConsumer<String, BasicMonster> kafkaConsumer;


    public MonsterConsumer() {
        kafkaConsumer = new KafkaConsumer<>(createConsumerProperties());
        kafkaConsumer.subscribe(Collections.singletonList(MONSTER_TOPIC));
    }

    public ConsumerRecords<String, BasicMonster> getDataFromKafka() {
        return kafkaConsumer.poll(1000);
    }

    private static Properties createConsumerProperties() {
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");
        properties.put("group.id", "customer-consumer-group-v1");
        properties.put("auto.commit.enable", "false");
        properties.put("auto.offset.reset", "earliest");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", KafkaAvroDeserializer.class.getName());
        properties.setProperty("schema.registry.url", "http://127.0.0.1:8081");
        properties.setProperty("specific.avro.reader", "true");
        return properties;
    }
}
