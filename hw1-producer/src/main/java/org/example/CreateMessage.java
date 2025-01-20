package org.example;

import org.apache.kafka.clients.producer.ProducerRecord;

public class CreateMessage {

    public static void main(String[] args) {

        Producer1 producer = new Producer1();

        for (int i = 1; i <= 2; i++) {
            // Отправка сообщения (указываем в какой топик отправить сообщение, ключ и значение сообщения)
            ProducerRecord<String, String> record = new ProducerRecord<>("topic-hw1",
                    "key-1", "Тест " + i);
            producer.sendMessage(record);
        }
    }
}
