package org.example;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaException;
import org.example.config.Producer1Config;

@Slf4j
public class Producer1 {

    Producer1Config producer1Config = new Producer1Config();

    public void sendMessage(ProducerRecord<String, String> record) {
        KafkaProducer<String, String> producer = new KafkaProducer<>(producer1Config.setProducerConfig());
        try {
            producer.send(record);
            log.info("Message sent: key = {}, value = {}, partition = {}",
                    record.key(), record.value(), record.partition());
        } catch (KafkaException ex) {
            log.warn("Error ", ex);
        } finally {
            producer.close();
        }
    }
}
