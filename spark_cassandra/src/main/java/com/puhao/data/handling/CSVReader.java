package com.puhao.data.handling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.catalyst.encoders.ExpressionEncoder;
import org.apache.spark.sql.catalyst.encoders.RowEncoder;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTime;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.UUID;

/**
 * Created by pu on 2017/11/24.
 */
public class CSVReader implements Serializable {
    private static Logger logger = LogManager.getLogger(CSVReader.class);


    public void readAndSaveToDB(String filePath) {

        Dataset<Row> datas = readCSVandConvertToTableStructure(Main.PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_HADOOP_HDFS_ROOT) + filePath);

        datas.show(10);
        logger.info("Total row count: {}", datas.count());

        /**
         * Save to cassandra table
         */
        datas.write().format("org.apache.spark.sql.cassandra")
                .option("table",Main.PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_CASSANDRA_TABLENAME))
                .option("keyspace", Main.PROPERTIES.getProperty(SparkConf.PROPERTY_KEY_CASSANDRA_KEYSPACE))
                .save();
    }

    public Dataset<Row> readCSVandConvertToTableStructure(String filePath){

        final String dateStr = Utils.getFileNameWithoutSuffix(filePath);
        logger.debug("Date string get from file name: {}", dateStr);
        DateTime dateTime = Main.fileNameDateFormatter.parseDateTime(dateStr);
        java.sql.Date date = new java.sql.Date(dateTime.getMillis());
        logger.info("Date get from file name: {}",Main.fileNameDateFormatter.print(dateTime));




        StructType tableStr = new StructType();
        tableStr = tableStr.add("item_id", DataTypes.StringType, false);
        tableStr = tableStr.add("stock_code", DataTypes.StringType, false);
        tableStr = tableStr.add("trading_date", DataTypes.DateType, true);
        tableStr = tableStr.add("item_value", DataTypes.DoubleType, false);
        ExpressionEncoder<Row> tableStrEncoder = RowEncoder.apply(tableStr);


        StructType csvStr = new StructType();
        csvStr = csvStr.add("stock_code", DataTypes.StringType, false);
        csvStr = csvStr.add("item_1_value", DataTypes.DoubleType, false);
        csvStr = csvStr.add("item_2_value", DataTypes.DoubleType, false);
        csvStr = csvStr.add("item_3_value", DataTypes.DoubleType, false);


        return sparkSession.read()
                .option("mode", "DROPMALFORMED") // skip invalid data line
                .option("header","true")  // let spark know we have header and skip it
                .schema(csvStr)
                .csv(filePath)
                .flatMap(row -> {

                    Object[] row1 = new Object[]{UUID.randomUUID().toString(), row.get(0), date, row.get(1)};
                    Object[] row2 = new Object[]{UUID.randomUUID().toString(), row.get(0), date, row.get(2)};
                    Object[] row3 = new Object[]{UUID.randomUUID().toString(), row.get(0), date, row.get(3)};

                    ArrayList<Row> newRow = new ArrayList<>(3);
                    newRow.add(RowFactory.create(row1));
                    newRow.add(RowFactory.create(row2));
                    newRow.add(RowFactory.create(row3));

                    return newRow.iterator();

                }, tableStrEncoder);
    }


    private SparkSession sparkSession;
    public CSVReader(SparkSession sparkSession){
        this.sparkSession = sparkSession;
    }

}
