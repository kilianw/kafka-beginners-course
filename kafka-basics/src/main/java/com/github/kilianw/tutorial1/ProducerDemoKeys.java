package com.github.kilianw.tutorial1;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class ProducerDemoKeys {
    public static void main(String[] args) throws ExecutionException, InterruptedException {
//        System.out.println("hello world!");
        final Logger logger = LoggerFactory.getLogger(ProducerDemoKeys.class.getName());

        // create properties
        Properties properties = new Properties();
        String bootstrapServers = "127.0.0.1:9092";
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 5; i++) {
            String topic = "first_topic";
            String value = "hello world" + Integer.toString(i);
            String key = "id_" + Integer.toString(i);
            logger.info("key: "+key);
            //create producer record
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            //send data - asynchronous
            producer.send(record, new Callback() {
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    // execute every time a record is sent or exception
                    if (exception == null) {
                        logger.info("Received new metadata. \n" +
                                "topic:" + metadata.topic() + "\n" +
                                "Partition " + metadata.partition() + "\n" +
                                "offset " + metadata.offset() + "\n" +
                                "timestamp " + metadata.timestamp());
                    } else {
                        logger.error(exception.getLocalizedMessage());
                    }
                }
            }).get();
        }
        producer.flush();
        producer.close();
    }
}
