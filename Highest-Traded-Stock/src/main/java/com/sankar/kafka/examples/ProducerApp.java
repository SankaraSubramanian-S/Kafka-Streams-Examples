package com.sankar.kafka.examples;

import com.fasterxml.jackson.databind.JsonNode;
import com.sankar.kafka.examples.producer.ProducerThread;
import com.sankar.kafka.examples.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.io.IOException;
import java.io.InputStream;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class ProducerApp {
    private static final Logger logger = LogManager.getLogger();
    private static final String kafkaConfig = "/kafka.properties";


    /**
     * Application entry point
     * you must provide the topic name and at least one event file
     *
     * @param args topicName (Name of the Kafka topic) list of files (list of files in the classpath)
     */
    @SuppressWarnings("unchecked")
    public static void main(String[] args) {

        if (args.length < 2) {
            System.out.println("Please provide command line arguments: topicName EventFiles");
            System.exit(-1);
        }
        String topicName = args[0];
        String[] eventFiles = Arrays.copyOfRange(args, 1, args.length);
        logger.trace("Creating Kafka producer...");
        Properties properties = new Properties();
        try {
            InputStream kafkaConfigStream = ClassLoader.class.getResourceAsStream(kafkaConfig);
            properties.load(kafkaConfigStream);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());

        } catch (IOException e) {
            logger.error("Cannot open Kafka config " + kafkaConfig);
            throw new RuntimeException(e);
        }
        /**
        *Creating Single producer and sharing it across thread instances
        */
        int noOfProducerThreads = Integer.parseInt(properties.getProperty(AppConfigs.noOfProducerKey));
        properties.remove(AppConfigs.noOfProducerKey);
        KafkaProducer<String, JsonNode> producer = new KafkaProducer<>(properties);
        ExecutorService executor = Executors.newFixedThreadPool(noOfProducerThreads);
        for (String fileName :eventFiles)
        {
            ProducerThread producerthread = new ProducerThread(producer, topicName, fileName);
            executor.submit(producerthread);
        }

        executor.shutdown();
        try {
            executor.awaitTermination(5000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            logger.info("Exception in shutting down ExecutorService "+e);
            throw new RuntimeException(e);
        }
        finally {
            producer.close();
            logger.info("Finished Application - Closing Kafka Producer.");
        }
    }

}