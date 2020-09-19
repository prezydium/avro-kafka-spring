package com.presidium.dev.avro.confluent;

import com.presidium.dev.BasicMonster;
import org.apache.kafka.clients.consumer.ConsumerRecords;

public class App {

    public static void main(String[] args) throws InterruptedException {
        MonsterProducer monsterProducer = new MonsterProducer();
        BasicMonster orc = BasicMonster.newBuilder()
                .setName("orc")
                .setAttack(5.5f)
                .setHealthPoints(30)
                .setArmor(4)
                .setLoot(12)
                .build();
        BasicMonster manticora = BasicMonster.newBuilder()
                .setName("manticora")
                .setAttack(33f)
                .setHealthPoints(250)
                .setArmor(15)
                .setLoot(1200)
                .build();

        monsterProducer.sendMonster(orc);
        monsterProducer.sendMonster(manticora);

        Thread.sleep(5000);

        MonsterConsumer monsterConsumer = new MonsterConsumer();
        ConsumerRecords<String, BasicMonster> dataFromKafka = monsterConsumer.getDataFromKafka();
        dataFromKafka.forEach(
                record -> System.out.println(record.value())
        );
    }

}
