package org.example.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Properties;

public class Consumer1Config {

    public Properties setConsumerConfig() {
        // Настройка консьюмера – адреса сервера, сериализаторы для ключа и значения
        Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094, localhost:9095, localhost:9096");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());

        // указываем/создаем группу консьюмера, чтобы каждый из 2 консьюмеров получал все сообщения
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");

        // определяем минимальный объем данных (в байтах) которые консьюмер должен получить за один запрос к брокеру
        properties.put(ConsumerConfig.FETCH_MIN_BYTES_CONFIG, 10 * 1024 * 1024);

        // при запуске консьюмера читаются все сообщения без фиксированных смещений для данной группы
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // отключаем автоматический коммит смещения
        properties.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        return properties;
    }
}
