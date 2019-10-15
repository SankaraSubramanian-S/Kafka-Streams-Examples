package com.sankar.kafka.examples;

import com.sankar.kafka.examples.type.StockData;
import com.sankar.kafka.examples.serde.JsonSerializer;
import com.sankar.kafka.examples.serde.JsonDeserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;

import java.util.HashMap;
import java.util.Map;

/**
 * Factory class for Serdes
 */

class StockDataSerdes extends Serdes {


    static final class StockDataSerde extends WrapperSerde<StockData> {
        StockDataSerde() {
            super(new JsonSerializer<>(), new JsonDeserializer<>());
        }
    }
    static Serde<StockData> StockData() {
        StockDataSerde serde = new StockDataSerde();
        Map<String, Object> serdeConfigs = new HashMap<>();
        serdeConfigs.put("specific.class.name", StockData.class);
        serde.configure(serdeConfigs, false);
        return serde;
    }

}
