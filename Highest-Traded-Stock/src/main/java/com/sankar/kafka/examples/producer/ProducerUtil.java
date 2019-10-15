package com.sankar.kafka.examples.producer;

import com.fasterxml.jackson.databind.MappingIterator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import com.sankar.kafka.examples.type.StockData;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;

import java.io.File;
import java.io.IOException;
import java.util.List;

public class ProducerUtil {

     static List<StockData> getStocks(String dataFile) throws IOException {

        File file = new File(dataFile);
        CsvMapper mapper = new CsvMapper();
        /*Data files contains first line as header
        and needs to be ignored */
        CsvSchema schema =  mapper.schemaFor(StockData.class).withLineSeparator("\n")
                .withColumnSeparator(',').withSkipFirstDataRow(true);
        MappingIterator<StockData> stockDataIterator = mapper.readerFor(StockData.class).with(schema).readValues(file);
        return stockDataIterator.readAll();
    }

    static String moveFiletoProcessed(String fileName) throws IOException {
        String processedfilename =
                FilenameUtils.concat(FilenameUtils.getFullPath(fileName),"processed/"+FilenameUtils.getName(fileName));
        FileUtils.moveFile(FileUtils.getFile(fileName), FileUtils.getFile(processedfilename));
        return processedfilename;
    }

}
