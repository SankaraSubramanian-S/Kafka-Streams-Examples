package com.sankar.kafka.examples;

import com.sankar.kafka.examples.type.StockData;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.*;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.text.SimpleDateFormat;
import java.util.Properties;

import static java.time.Duration.ofMinutes;
import static java.time.Duration.ofSeconds;
import static org.apache.kafka.streams.kstream.Suppressed.BufferConfig.maxBytes;
import static org.apache.kafka.streams.kstream.Suppressed.untilTimeLimit;

public class StreamsApp {

    private static final Logger logger = LogManager.getLogger();

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, AppConfigs.applicationID);
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, AppConfigs.bootstrapServers);
        props.put(StreamsConfig.STATE_DIR_CONFIG, "state-store");

        StreamsBuilder streamsBuilder = new StreamsBuilder();
        KStream<String, StockData> KS0 = streamsBuilder.stream(AppConfigs.TopicName,
                Consumed.with(StockDataSerdes.String(), StockDataSerdes.StockData()));

        SimpleDateFormat sdf  = new SimpleDateFormat("MM/dd/yyyy");
        KStream<String, StockData> KS1 = KS0.map(
                (key, stock) -> new KeyValue<>(sdf.format(stock.getTradeDate()),stock)
        );

        KGroupedStream<String, StockData> KGS2 = KS1.groupByKey(
                Grouped.with(StockDataSerdes.String(),StockDataSerdes.StockData())
        );

        KTable<String, StockData> KTS3 = KGS2.reduce(
                (aggValue, newValue) -> {
                    if (newValue.getTotalTradedVal() > aggValue.getTotalTradedVal())
                        return newValue;
                    else
                        return aggValue;
                }
        );
        /**
        * Suppressing the KTable emit rate to 30 seconds or 1 KB data limit
         * to reduce network congestion and disk operations.
         * Can be tuned as per requirements
         * Applicable only when final state is required and
         * intermediate states can be ignored
        * */
        KTS3//.suppress(untilTimeLimit(ofSeconds(30), maxBytes(1000L).emitEarlyWhenFull()))
                .toStream()
                .peek((k,v) -> System.out.println("key is :"+k+" value is : "+v))
                .to(AppConfigs.outputTopic,Produced.with(StockDataSerdes.String(),StockDataSerdes.StockData()));


        logger.info("Starting Kafka Streams");
        KafkaStreams myStream = new KafkaStreams(streamsBuilder.build(), props);
        myStream.start();


        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            logger.info("Stopping Stream");
            myStream.close();
        }));

    }
}
