package org.example;

import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;

public class Consumer1 {

    // Консьюмер 1 реализует:
    // 1) pull - модель получения данных (10 секунд, между опросами)
    // 2) вручную управляемый коммит смещения

    public static void main(String[] args) {
        // Настройка консьюмера – адреса сервера, сериализаторы для ключа и значения
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094, localhost:9095, localhost:9096");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // указываем/создаем группу консьюмера, чтобы каждый из 2 консьюмеров получал все сообщения
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");

        // определяем минимальный объем данных (в байтах) которые консьюмер должен получить за один запрос к брокеру
        props.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10 * 1024 * 1024);

        // при запуске консьюмера читаются все сообщения без фиксированных смещений для данной группы
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // отключаем автоматический коммит смещения
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");

        // создаем консьюмера с настрйками заданными выше
        KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);

        // Подписка на топик
        consumer.subscribe(Collections.singletonList("topic-hw1"));

        // бесконечный цикл для обращения к брокеру
        while (true) {
            try {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(10000));
                for (ConsumerRecord<String, String> record : records) {
                    System.out.printf("Получено сообщение: key = %s, value = %s, partition = %s, offset = %d%n",
                            record.key(), record.value(), record.partition(), record.offset());
                }
                try {
                    // после завершения обработки полученных сообщений вызываем метод commitSync()
                    // для фиксации последнего смещения, прежде чем выполнить опрос для получения
                    // дополнительных сообщений
                    consumer.commitSync();
                } catch (CommitFailedException ex) {
                    System.out.println("Ошибка фиксации смещения");
                }
            } catch (SerializationException ex) {
                System.out.println("Ошибка десериализации");
                consumer.close();
            }
        }
    }
}
