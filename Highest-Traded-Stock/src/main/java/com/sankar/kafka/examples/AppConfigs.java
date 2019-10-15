package com.sankar.kafka.examples;

/**
 * Configurations for the KafkaRequirement POC
*/
class AppConfigs {

    final static String applicationID = "HighestTradedStock";
    final static String bootstrapServers = "localhost:9092";
    final static String TopicName = "stockdata";
    final static String outputTopic = "highest-traded-stock";
    final static String noOfProducerKey = "producer.threads";
}
