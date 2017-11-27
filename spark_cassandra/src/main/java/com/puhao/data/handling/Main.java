package com.puhao.data.handling;

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.SparkSession;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;

/**
 * Created by pu on 2017/11/25.
 */
public class Main {

    private static Logger logger = LogManager.getLogger(Main.class);


    public static DateTimeFormatter fileNameDateFormatter;

    public static Properties PROPERTIES;
    private static String PROPERTIES_FILE_PATH = "application.properties";
    /**
     * @param args the file path in HDFS, like "/user/puhao/2017-09-19.csv"
     * @throws IOException
     */
    public static void main(String[] args) throws IOException {
        Main.initializeProperties();

        /**
         * File full path in HDFS should pass through command line argument
         */
        Pattern fileNamePattern = Pattern.compile(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_FILE_NAME_PATTERN));
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


        final SparkSession spark = SparkConf.getSparkSession(Main.PROPERTIES);
        logger.info("Spark session created");


        CSVReader reader = new CSVReader(spark);
        for(String filePath : filePathList) {
            logger.info("Start to load data in {}", filePath);

            reader.readAndSaveToDB(filePath);

            logger.info("{} data loaded to DB at {}", filePath, PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST));
        }
    }

    private static void initializeProperties() throws IOException {
        PROPERTIES = new Properties();
        PROPERTIES.load(CSVReader.class.getClassLoader().getResourceAsStream(PROPERTIES_FILE_PATH));

        /**
         * Print all the properties in log for troubleshooting propose
         */
        logger.info("Application properties loaded.");
        for(String pName : PROPERTIES.stringPropertyNames()){
            if(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD.equals(pName)){
                /**
                 * Password should not show in any log file
                 */
                logger.debug("{} - ********", SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD);
            }else{
                logger.debug("{} - {}", pName, PROPERTIES.getProperty(pName));
            }
        }

        /**
         * Ensure all properties is provided
         */
        List<String> missingMandatoryProperties = new ArrayList<>(6);
        if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_CASSANDRA_KEYSPACE))){
            missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_CASSANDRA_KEYSPACE);
        }
        if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_CASSANDRA_TABLENAME))){
            missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_CASSANDRA_TABLENAME);
        }
        if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_SPARK_MASTER_URL))){
            missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_SPARK_MASTER_URL);
        }
        if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST))){
            missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST);
        }
        if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_FILE_NAME_DATE_PATTERN))){
            missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_FILE_NAME_DATE_PATTERN);
        }
        if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_FILE_NAME_PATTERN))){
            missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_FILE_NAME_PATTERN);
        }
        if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_HADOOP_HDFS_ROOT))){
            missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_HADOOP_HDFS_ROOT);
        }
        if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_SPARK_APP_NAME))){
            missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_SPARK_APP_NAME);
        }
        if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_SPARK_APP_NAME))){
            missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_SPARK_APP_NAME);
        }

        if(! Boolean.parseBoolean(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_ANONYMOUS_LOGIN))){
            /**
             * If not anonymous login
             */
            if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_USERNAME))){
                missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_USERNAME);
            }
            if(StringUtils.isBlank(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD))){
                missingMandatoryProperties.add(SparkConf.PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD);
            }
        };



        if(missingMandatoryProperties.size() > 0){
            throw  new IllegalArgumentException("Mandatory properties is not provide: " + missingMandatoryProperties.toString());
        }

        fileNameDateFormatter = DateTimeFormat.forPattern(PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_FILE_NAME_DATE_PATTERN));
        //.withZone(DateTimeZone.forOffsetHours(8));// use UTC+8 timezone



        if(StringUtils.isNotBlank(PROPERTIES.getProperty("hadoop.home.dir"))) {
            System.setProperty("hadoop.home.dir", PROPERTIES.getProperty("hadoop.home.dir"));
        }
    }

}
