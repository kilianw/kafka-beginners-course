package com.github.kilianw.tutorial1.twitter;


import com.google.common.collect.Lists;
import com.twitter.hbc.ClientBuilder;
import com.twitter.hbc.core.Client;
import com.twitter.hbc.core.Constants;
import com.twitter.hbc.core.Hosts;
import com.twitter.hbc.core.HttpHosts;
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint;
import com.twitter.hbc.core.event.Event;
import com.twitter.hbc.core.processor.StringDelimitedProcessor;
import com.twitter.hbc.httpclient.auth.Authentication;
import com.twitter.hbc.httpclient.auth.OAuth1;
import com.twitter.hbc.twitter4j.Twitter4jStatusClient;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import twitter4j.*;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.*;

public class TwitterProducer {

    private Logger logger = LoggerFactory.getLogger(TwitterProducer.class.getName());

    public static void main(String[] args) throws InterruptedException {
        TwitterProducer p = new TwitterProducer();
        p.setup();
        p.stream();
        p.shutdown();
    }

    private Client hosebirdClient;
    private Twitter4jStatusClient t4jClient;


    private Properties properties = new Properties();
    private String bootstrapServers = "127.0.0.1:9092";
    private String kafkaTopic = "TwitterStatusTopic";
    private KafkaProducer<String, String> producer;

    /* Set up your blocking queues: Be sure to size these properly based on expected TPS of your stream */
    BlockingQueue<String> msgQueue = new LinkedBlockingQueue<>(100000);
    BlockingQueue<Event> eventQueue = new LinkedBlockingQueue<Event>(1000);

    public void setup() {
        setupKafka();
        setupTwitter();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            shutdown();
        }));
    }

    public void setupKafka() {

        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true");
        properties.setProperty(ProducerConfig.ACKS_CONFIG, "all");
        properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE));
        properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5");
        properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy");
        properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20");
        properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32*1024));


        producer = new KafkaProducer<>(properties);
    }

    public void setupTwitter() {

        Properties prop = new Properties();
        try (InputStream input = TwitterProducer.class.getClassLoader().getResourceAsStream("config.properties")) {
            if (input == null) {
                System.out.println("Sorry, unable to find config.properties");
                return;
            }
            //load a properties file from class path, inside static method
            prop.load(input);

        } catch (IOException ex) {
            ex.printStackTrace();
        }

        /* Declare the host you want to connect to, the endpoint, and authentication (basic auth or oauth) */
        Hosts hosebirdHosts = new HttpHosts(Constants.STREAM_HOST);
        StatusesFilterEndpoint hosebirdEndpoint = new StatusesFilterEndpoint();
        /* Optional: set up some followings and track terms */
        List<Long> followings = Lists.newArrayList(1234L, 566788L);
        List<String> terms = Lists.newArrayList("bitcoin","usa", "poltics");
        hosebirdEndpoint.followings(followings);
        hosebirdEndpoint.trackTerms(terms);

        int numProcessingThreads = 4;
        ExecutorService executorService = Executors.newFixedThreadPool(numProcessingThreads);
        // These secrets should be read from a config file
        Authentication hosebirdAuth = new OAuth1(prop.getProperty("consumerKey"), prop.getProperty("consumerSecret"), prop.getProperty("accessToken"), prop.getProperty("accessSecret"));

        ClientBuilder builder = new ClientBuilder()
                .name("TwitterKafkaElastic")                              // optional: mainly for the logs
                .hosts(hosebirdHosts)
                .authentication(hosebirdAuth)
                .endpoint(hosebirdEndpoint)
                .processor(new StringDelimitedProcessor(msgQueue))
                .eventMessageQueue(eventQueue);                          // optional: use this if you want to process client events

        hosebirdClient = builder.build();
//        t4jClient = new Twitter4jStatusClient(hosebirdClient, msgQueue, Lists.newArrayList(listener1), executorService);
//        t4jClient.connect();
//
//        for (int threads = 0; threads < numProcessingThreads; threads++) {
//            // This must be called once per processing thread
//            t4jClient.process();
//        }
//
//        try {
//            Thread.sleep(5000);
//        } catch (InterruptedException e) {
//            e.printStackTrace();
//        }

//        hosebirdClient.stop();
// Attempts to establish a connection.
        hosebirdClient.connect();
        logger.info("Setup Complete");
    }

    public void stream() {
        while (!hosebirdClient.isDone()) {
//        while (true) {
            String msg = null;
            try {
                msg = msgQueue.poll(5, TimeUnit.SECONDS);
                logger.info(msg);
                setMsgToKafka(msg);
            } catch (InterruptedException e) {
                logger.error("Error streaming", e);
                e.printStackTrace();
                shutdown();
            }
            // one msg only
//            shutdown();
        }
    }

    public void setMsgToKafka(String msg) {
        ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, msg);
        producer.send(record, (RecordMetadata metadata, Exception exception) -> {
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
        );
    }

    public void shutdown() {
        hosebirdClient.stop();
        producer.close();
        logger.info("Shutdown Complete");
    }

    private StatusListener listener1 = new StatusListener() {
        @Override
        public void onStatus(Status status) {
            logger.info("onStatus ID: {} Text: {} User: {}", status.getId(), status.getText(), status.getUser().getName());

            ProducerRecord<String, String> record = new ProducerRecord<String, String>(kafkaTopic, status.getUser().getName(), status.toString());
            producer.send(record, (RecordMetadata metadata, Exception exception) -> {
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
            );
        }

        @Override
        public void onDeletionNotice(StatusDeletionNotice statusDeletionNotice) {
            logger.info("onDeletionNotice", statusDeletionNotice);
        }

        @Override
        public void onTrackLimitationNotice(int limit) {
            logger.info("onDonTrackLimitationNoticeeletionNotice", limit);
        }

        @Override
        public void onScrubGeo(long user, long upToStatus) {
            logger.info("onScrubGeo", user, upToStatus);
        }

        @Override
        public void onStallWarning(StallWarning warning) {
            logger.info("onStallWarning", warning);
        }

        @Override
        public void onException(Exception e) {
            logger.info("onException", e);
        }
    };
}
