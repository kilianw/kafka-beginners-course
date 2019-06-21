package com.github.kilianw.tutorial1.elasticsearch;

import org.apache.http.HttpHost;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.elasticsearch.action.index.IndexRequest;
import org.elasticsearch.action.index.IndexResponse;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.common.xcontent.XContentType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

public class ElasticSearchConsumer {

    public static void main(String[] args) throws IOException {
        ElasticSearchConsumer c = new ElasticSearchConsumer();
        c.setup();
        c.run();
        c.shutdown();
    }

    private Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.class.getName());

    private String bootstrapServers = "127.0.0.1:9092";
    private String groupId = "ESConsumer";
    private String topic = "TwitterStatusTopic";
    private RestHighLevelClient esClient;


    public void run() {

        CountDownLatch latch = new CountDownLatch(1);
        Runnable ct = new ElasticSearchConsumer.ConsumerThread(latch, topic, bootstrapServers, groupId);
        //start thread
        Thread myThread = new Thread(ct);
        myThread.start();

        // shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Caught shutdown hook");
            ((ElasticSearchConsumer.ConsumerThread) ct).shutdown();
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

        private Logger logger = LoggerFactory.getLogger(ElasticSearchConsumer.ConsumerThread.class.getName());
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
                        IndexRequest request = new IndexRequest("twitter");
                        request.source(record.value(), XContentType.JSON);
//                        AcknowledgedResponse putMappingResponse = esClient.indices().putMapping(request, RequestOptions.DEFAULT);
                        IndexResponse response = esClient.index(request, RequestOptions.DEFAULT);
                        logger.info("ES response {}", response);
                    }
                }
            } catch (WakeupException e) {
                logger.info("Received shutdown signal");

            } catch (IOException e) {
                e.printStackTrace();
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

    public void setup() {
        esClient = new RestHighLevelClient(
                RestClient.builder(
                        new HttpHost("localhost", 9200, "http")));
    }

    public void shutdown() throws IOException {
        esClient.close();
    }
}