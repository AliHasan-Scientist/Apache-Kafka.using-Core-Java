package com.learn.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.learn.kafka.config.ProducerProperties;
import com.learn.kafka.topic.TopicNames;

public class ProducerWithKeys {

    public static void main(String[] args) {
        ProducerProperties configs = new ProducerProperties();
        final Logger logger = LoggerFactory.getLogger(ProducerWithKeys.class.getSimpleName());
        final KafkaProducer<String, String> producer = new KafkaProducer<>(configs.setProducerProperties());
        TopicNames names = new TopicNames();

        final ProducerRecord<String, String> record = new ProducerRecord<String, String>(names.ALFA_BETA, "name: ",
                "roy ali hasan");
        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info("Topic: " + metadata.topic());
                    logger.info("Key Of Topic: " + record.key());
                    logger.info("Value Of Topic: " + record.value());
                } else {
                    logger.error(exception.getMessage());
                }

            }

        });
        producer.flush();
        producer.close();

    }
}
