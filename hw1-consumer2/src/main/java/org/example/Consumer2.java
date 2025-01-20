package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.example.config.Consumer2Config;

import java.time.Duration;
import java.util.Collections;

@Slf4j
public class Consumer2 {

    // Консьюмер 2 реализует:
    // 1) "push" - модель получения данных (5 миллисекунд, между опросами)
    // 2) автоматический коммит смещения

    private final static Duration TIMEOUT = Duration.ofMillis(5);

    public static void main(String[] args) {

        // создаем экземпляр класса конфигурации консьюмера
        Consumer2Config consumer2Config = new Consumer2Config();

        // создаем консьюмера с настрйками из класса ConsumerConfig
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumer2Config.setConsumerConfig());

        // Подписка на топик
        consumer.subscribe(Collections.singletonList("topic-hw1"));

        // бесконечный цикл для обращения к брокеру
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(TIMEOUT);
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Message received: key = {}, value = {}, partition = {}, offset = {}",
                            record.key(), record.value(), record.partition(), record.offset());
                }
            } catch (KafkaException ex) {
                log.warn("Error ", ex);
                consumer.close();
            }
        }
    }
}


