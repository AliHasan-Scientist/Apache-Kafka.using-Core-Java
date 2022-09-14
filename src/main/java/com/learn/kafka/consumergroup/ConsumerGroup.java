package com.learn.kafka.consumergroup;

import java.time.Duration;
import java.util.Arrays;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.learn.kafka.config.ConsumerProperties;
import com.learn.kafka.topic.TopicNames;

public class ConsumerGroup {
    public static void main(String[] args) {
        final Logger logger = LoggerFactory.getLogger(ConsumerGroup.class.getSimpleName());
        ConsumerProperties configs = new ConsumerProperties();
        configs.setGroup("my_second_application");

        TopicNames topics = new TopicNames();

        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(configs.setConsumerProperties());
        // get reference to the current Thread
        final Thread mainThread = Thread.currentThread();
        // by adding the shutdown Hook
        Runtime.getRuntime().addShutdownHook(new Thread() {
            public void run() {
                logger.info("Detected a shutdown, let's exit by calling consumer.wakeup()....");
                consumer.wakeup();
                // join the mainTread to allow the execution in the main thread
                try {
                    mainThread.join();
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
        });

        // subscribe the consumer
        consumer.subscribe(Arrays.asList(topics.ALFA_TOPIC));

        try {
            // poll for new data
            while (true) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
                for (ConsumerRecord<String, String> record : records) {
                    logger.info("Topic Name:" + record.topic());
                    logger.info("Key of Consumer:" + record.key());
                    logger.info("Value of Consumer:" + record.value());

                }
            }
        } catch (WakeupException e) {
            logger.info("Wakeup Exception");
            // we ignore this as this is an expected exception when closing the consumer

        } catch (Exception exception) {
            logger.error("Unexpected exception ");
        } finally {
            consumer.close(); // this will also commit the offsets if need to be
            logger.info("This consumer is now gracefully closed!");
        }
    }
}