package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaException;
import org.example.config.Consumer1Config;

import java.time.Duration;
import java.util.Collections;

@Slf4j
public class Consumer1 {

    // Консьюмер 1 реализует:
    // 1) pull - модель получения данных (10 секунд, между опросами)
    // 2) вручную управляемый коммит смещения

    private final static Duration TIMEOUT = Duration.ofMillis(10000);

    public static void main(String[] args) {

        // создаем экземпляр класса конфигурации консьюмера
        Consumer1Config consumer1Config = new Consumer1Config();

        // создаем консьюмера с настрйками из класса ConsumerConfig
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consumer1Config.setConsumerConfig());

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
                try {
                    // после завершения обработки полученных сообщений вызываем метод commitSync()
                    // для фиксации последнего смещения, прежде чем выполнить опрос для получения
                    // дополнительных сообщений
                    consumer.commitSync();
                } catch (KafkaException ex) {
                    log.warn("Commit failed ", ex);
                }
            } catch (KafkaException ex) {
                log.warn("Error ", ex);
                consumer.close();
            }
        }
    }
}
