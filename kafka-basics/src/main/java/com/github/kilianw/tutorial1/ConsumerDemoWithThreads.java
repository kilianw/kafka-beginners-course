package com.github.kilianw.tutorial1;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ConsumerDemoWithThreads {

    private Logger logger = LoggerFactory.getLogger(ConsumerDemoWithThreads.class.getName());

    public static void main(String[] args) {
        System.out.println("Hello world");
        new ConsumerDemoWithThreads().run();

    }


    public void run() {

        String bootstrapServers = "127.0.0.1:9092";
        String groupId = "mySixthApplication";
        String topic = "first_topic";
        CountDownLatch latch = new CountDownLatch(1);
        Runnable ct = new ConsumerThread(latch, topic, bootstrapServers, groupId);
        //start thread
        Thread myThread = new Thread(ct);
        myThread.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ConsumerThread) ct).shutdown();
            try {
                latch.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            logger.info("App exited");
        }));

        try {
            latch.await();
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Closing");
        }
    }


    public class ConsumerThread implements Runnable {

        private Logger logger = LoggerFactory.getLogger(ConsumerThread.class.getName());
        private CountDownLatch latch;
        private KafkaConsumer<String, String> consumer;

        public ConsumerThread(CountDownLatch latch, String topic, String bootstrapServers, String groupId) {
            this.latch = latch;

            Properties properties = new Properties();
            properties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            properties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
            properties.setProperty(ConsumerConfig.GROUP_ID_CONFIG, groupId);
            properties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
            this.consumer = new KafkaConsumer<String, String>(properties);
            // subscriber consumer to topic(s)
            consumer.subscribe(Collections.singleton(topic));
        }

        @Override
        public void run() {
            try {
                // poll for new data
                while (true) {
                    ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));

                    for (ConsumerRecord<String, String> record : records) {
                        logger.info("key: " + record.key() + ", Value:" + record.value());
                        logger.info("Partition: " + record.partition() + ", offset:" + record.offset());
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");

            } finally {
                consumer.close();
                // tell main code we're done with consumer
                latch.countDown();
            }
        }

        public void shutdown() {
            consumer.wakeup();
        }
    }
}
