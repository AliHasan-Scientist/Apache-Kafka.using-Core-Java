package com.learn.kafka.config;

import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConsumerProperties {
    private String group;

    public Properties setConsumerProperties() {
        final Logger logger = LoggerFactory.getLogger(ConsumerProperties.class.getSimpleName());
        Properties configs = new Properties();
        configs.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        configs.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        configs.setProperty(ConsumerConfig.GROUP_ID_CONFIG, group);
        configs.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // Logging
        logger.trace("Consumer Properties Configuration Working .....");

        return configs;

    }

    public void setGroup(String group) {
        this.group = group;
    }
}
