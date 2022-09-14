package com.learn.kafka.kafkaconsumer;
import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.learn.kafka.config.ConsumerProperties;
import com.learn.kafka.topic.TopicNames;
public class ConsumerWithShuttdown {
    public static void main(String[] args) {
        Logger logger = LoggerFactory.getLogger(ConsumerWithShuttdown.class.getSimpleName());
        ConsumerProperties configs = new ConsumerProperties();
        TopicNames topics = new TopicNames();
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs.setConsumerProperties());

        // subscribe the consumer
        consumer.subscribe(Arrays.asList(topics.ALFA_TOPIC));

        // poll for new data
        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Topic Name:" + record.topic());
                logger.info("Key of Consumer:" + record.key());
                logger.info("Value of Consumer:" + record.value());

            }
        }
    }
}
