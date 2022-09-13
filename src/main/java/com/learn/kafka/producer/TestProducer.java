package com.learn.kafka.producer;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.learn.kafka.config.ProducerProperties;
import com.learn.kafka.topic.TopicNames;

public class TestProducer {

    public static void main(String[] args) {

        ProducerProperties configs = new ProducerProperties();
        TopicNames names = new TopicNames();
        KafkaProducer<String, String> producer = new KafkaProducer<>(configs.setProducerProperties());
        final ProducerRecord<String, String> record = new ProducerRecord<>(names.ALFA_TOPIC, "Hello");

        final Logger logger = LoggerFactory.getLogger(TestProducer.class.getSimpleName());
        producer.send(record, new Callback() {

            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info("Topic:" + metadata.topic());
                    logger.info("No of Partitions:" + metadata.partition());
                    logger.info("Topic Timestamp:" + metadata.timestamp());
                    logger.info("Topic Offset:" + metadata.offset());
                    logger.info("Key of the Topic:" + record.key());
                    logger.info("Value of the Topic:" + record.value());

                } else {

                    logger.error("%d:", exception.getMessage());
                }

            }

        });
        producer.flush();
        producer.close();

    }

}
