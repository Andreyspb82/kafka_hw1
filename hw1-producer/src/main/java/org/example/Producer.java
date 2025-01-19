package org.example;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.config.TopicConfig;
import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    // запускаем метод main который в цикле создает и отправляет сообщения в кластер брокеров
    public static void main(String[] args) {
        for (int i = 1; i <= 150; i++) {
            consumer(i);
        }
    }

    public static void consumer(int i) {
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

        // Создание продюсера
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);

        // Отправка сообщения (указываем в какой топик отправить сообщение, ключ и значение сообщения)
        try {
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-hw1", "key-1", "Тест " + i);
            producer.send(record);
            System.out.printf("Отправлено сообщение: key = %s, value = %s, partition =%d%n  ", record.key(),
                    record.value(), record.partition());
            // Закрытие продюсера
//            producer.close();
        } catch (SerializationException ex) {
            System.out.println("Ошибка сериализации");
        } finally {
            producer.close();
        }
    }
}
