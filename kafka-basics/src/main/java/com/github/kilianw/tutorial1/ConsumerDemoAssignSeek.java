package com.github.kilianw.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;

public class ConsumerDemoAssignSeek {
    public static void main(String[] args) {
        System.out.println("Hello world");
        String bootstrapServers = "127.0.0.1:9092";
//        String groupId = "myGroupApplication";
        String topic = "first_topic";
        Logger logger = LoggerFactory.getLogger(ConsumerDemoAssignSeek.class.getName());
        Properties properties = new Properties();
        properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
//        properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // create consumer
        KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(properties);
        TopicPartition partition = new TopicPartition(topic, 0);
        long offsetToReadFrom = 5L;
        consumer.assign(Arrays.asList(partition));
        consumer.seek(partition, offsetToReadFrom);

        int numOfMessagesToRead = 5;
        boolean keeOnReading = true;
        int readSoFar = 0;
        // poll for new data
        while (keeOnReading) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

            for (ConsumerRecord<String, String> record : records) {
                logger.info("key: " + record.key() + ", Value:" + record.value());
                logger.info("Partition: " + record.partition() + ", offset:" + record.offset());
            }
            readSoFar++;
            if (readSoFar >= numOfMessagesToRead) {
                keeOnReading = false;
            }
        }
    }
}
