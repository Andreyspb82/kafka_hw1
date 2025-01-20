package org.example.config;

import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer1Config {

    public Properties setProducerConfig() {
        // Конфигурация продюсера
        Properties properties = new Properties();

        // указываем порты 3 брокеров, используемых в 1 кластере
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9094, localhost:9095, localhost:9096");

        // указываем параметры сериализации (ключ и данные имеют тип данных String)
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // чтобы гарантировать доставку сообщения "Как минимум один раз" настраиваем следующие 3 параметра:
        // устанавливаем значение all, чтобы все брокеры подтвердили получение сообщения
        properties.put(ProducerConfig.ACKS_CONFIG, "all");

        // задаем количество повторных попыток при отправке сообщения
        properties.put(ProducerConfig.RETRIES_CONFIG, 3);

        // указываем минимальное число реплик которые должны подтвердить получение сообщения
        properties.put(TopicConfig.MIN_IN_SYNC_REPLICAS_CONFIG, "2");
        return properties;
    }
}
