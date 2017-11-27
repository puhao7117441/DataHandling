package com.puhao.data.handling;

import com.puhao.data.handling.common.Utils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.io.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.UUID;
import java.util.regex.Pattern;

/**
 * Created by pu on 2017/11/26.
 */
@Component
public class CSVReader {

    private static Logger logger = LogManager.getLogger(CSVReader.class);



    @Value("${application.csv.file.name.pattern}")
    private String fileNamePatternStr;

    @Value("${application.csv.file.name.date.pattern}")
    private String dateTimePattern;

    private DateTimeFormatter fileNameDateFormatter;


    @Autowired
    ItemRepository repo;

    public void test(){
        try {
            ItemEntity item = new ItemEntity();
            item.item_id = UUID.randomUUID().toString();
            item.stock_code = "10086";
            item.trading_date = Calendar.getInstance().getTime();
            item.item_value = 0.123412d;

            System.out.println("Repo: " + repo);

            System.out.println("Print existing");
            repo.findAll().forEach(i -> System.out.println(i.item_id));

            System.out.println("Save new");
            System.out.println("Save return: " + repo.save(item));

            System.out.println("Data saved");

            System.out.println("Count: " + repo.count());
            repo.findAll().forEach(i -> System.out.println(i.item_id));
        }catch (Exception e){
            e.printStackTrace();
        }

    }

    public void readCSV(String[] args) {


        Pattern fileNamePattern = Pattern.compile(fileNamePatternStr);
        List<String> filePathList = Utils.getAllMatchStringFromArray(args, fileNamePattern);

        if(filePathList.size() < args.length){
            /**
             * Log the invalid arguments
             */
            for(String arg : args){
                if(!filePathList.contains(arg)){
                    logger.error("Input file path is invalid: {}", arg);
                }
            }
        }

        if(filePathList.size() == 0){
            logger.info("No file path given from program arguments, do nothing");
            return;
        }


        for(String filePath : filePathList) {
            logger.info("Start to load data in {}", filePath);

            try {
                this.readAndSaveToDB(filePath, true);
                logger.info("{} data loaded to DB", filePath);
            } catch (IOException e) {
                logger.error("Fail to read file "+filePath, e);
            }

        }


    }

    private void readAndSaveToDB(String filePath, boolean hasHeader) throws IOException {

        final String dateStr = Utils.getFileNameWithoutSuffix(filePath);
        logger.debug("Date string get from file name: {}", dateStr);

        if(fileNameDateFormatter == null){
            fileNameDateFormatter = DateTimeFormat.forPattern(this.dateTimePattern);
        }

        DateTime dateTime = fileNameDateFormatter.parseDateTime(dateStr);
        logger.info("Date get from file name: {}",fileNameDateFormatter.print(dateTime));


        int bufferSize = 1000*3; // Because each line have 3 value, so make our buffer *3
        List<ItemEntity> itemList = new ArrayList<>(bufferSize);

        try(BufferedReader reader = new BufferedReader(new FileReader(new File(filePath)))){
            boolean isFirstLine = true;

            String line = null;
            String data[] = null;
            while(null != (line = reader.readLine())){
                if(hasHeader && isFirstLine){
                    isFirstLine = false;
                    continue;
                }

                data = line.split(",");
                if(data.length != 4){
                    logger.warn("Ignore invalid data line {} in file {}", line, filePath);
                    continue;
                }

                double d1,d2,d3;
                try {
                    /**
                     * Just verify if data is valid double
                     */
                    d1 = Double.parseDouble(data[1]);
                    d2 = Double.parseDouble(data[2]);
                    d3 = Double.parseDouble(data[3]);
                }catch (NumberFormatException e){
                    logger.warn("Ignore invalid (not double value) line {} in file {}", line, filePath);
                    continue;
                }



                itemList.add(new ItemEntity(UUID.randomUUID().toString(), dateTime.toDate(), data[0], d1));
                itemList.add(new ItemEntity(UUID.randomUUID().toString(), dateTime.toDate(), data[0], d2));
                itemList.add(new ItemEntity(UUID.randomUUID().toString(), dateTime.toDate(), data[0], d3));

                if(itemList.size() >= bufferSize){
                    logger.debug("Save {} record to DB", itemList.size() );
                    repo.saveAll(itemList);
                    itemList.clear();
                }


            }

            if(itemList.size() > 0){
                logger.debug("Save {} record to DB", itemList.size() );
                repo.saveAll(itemList);
            }

        }


    }
}
