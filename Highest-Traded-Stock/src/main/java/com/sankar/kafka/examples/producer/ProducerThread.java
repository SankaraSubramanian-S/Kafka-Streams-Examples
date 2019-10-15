package com.sankar.kafka.examples.producer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sankar.kafka.examples.type.StockData;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public class ProducerThread implements Runnable {

    private final KafkaProducer<String, JsonNode> producer;
    private final String topicName;
    private final String fileName;
    private static final Logger logger = LogManager.getLogger();

    public ProducerThread(KafkaProducer<String, JsonNode> producer, String topicName, String fileName) {
        this.producer = producer;
        this.topicName = topicName;
        this.fileName = fileName;
    }

    @Override
    public void run() {
        int messageCounter = 0;
        final ObjectMapper objectMapper = new ObjectMapper();
        List<JsonNode> stockArrayOfList = new ArrayList<>();
        try {
            for (StockData s : ProducerUtil.getStocks(fileName)) {
                stockArrayOfList.add(objectMapper.valueToTree(s));
            }
        } catch (IOException e) {
            logger.error("Error while reading input file "+e);
            throw new RuntimeException(e);
        }
        String producerName = Thread.currentThread().getName();
        logger.trace("Starting Producer thread" + producerName);
        for (JsonNode data : stockArrayOfList) {
            producer.send(new ProducerRecord<>(topicName, producerName+":"+fileName, data));
            messageCounter++;
        }
        logger.trace("Finished Producer thread" + producerName + " sent " + (messageCounter -1) + " messages");

        /**
         * Input File will be moved to processed folder post processing
         */
        try {
            String processedfilename= ProducerUtil.moveFiletoProcessed(fileName);
            logger.trace("Input file "+fileName+ "have been moved to prcessed path "+processedfilename);
        } catch (IOException e) {
            logger.error("Error while moving the file to processed directory "+ e);
            throw new RuntimeException(e);
        }


    }

}
