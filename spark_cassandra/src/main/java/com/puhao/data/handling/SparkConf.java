package com.puhao.data.handling;

import org.apache.spark.sql.SparkSession;

import java.util.Properties;

/**
 * Created by pu on 2017/11/25.
 */
public class SparkConf {
    public static final String PROPERTY_KEY_SPARK_MASTER_URL = "spark.master.url";
    public static final String PROPERTY_KEY_SPARK_APP_NAME= "spark.appName";
    public static final String PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST= "spark.cassandra.connection.host";
    public static final String PROPERTY_KEY_CASSANDRAN_ANONYMOUS_LOGIN= "spark.cassandra.auth.anonymous.login";
    public static final String PROPERTY_KEY_CASSANDRAN_AUTH_USERNAME= "spark.cassandra.auth.username";
    public static final String PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD= "spark.cassandra.auth.password";

    public static final String PROPERTY_KEY_FILE_NAME_PATTERN       ="application.csv.file.name.pattern";
    public static final String PROPERTY_KEY_FILE_NAME_DATE_PATTERN = "application.csv.file.name.date.pattern";
    public static final String PROPERTY_KEY_HADOOP_HDFS_ROOT="hadoop.hdfs.root";

    public static final String PROPERTY_KEY_CASSANDRA_KEYSPACE="cassandra.keyspace";
    public static final String PROPERTY_KEY_CASSANDRA_TABLENAME="cassandra.tablename";




    public static SparkSession getSparkSession(Properties properties) {
        /**
         * Build spark session
         */
        final SparkSession.Builder builder = SparkSession.builder()
                .master(properties.getProperty(PROPERTY_KEY_SPARK_MASTER_URL))
                .appName(properties.getProperty(PROPERTY_KEY_SPARK_APP_NAME))
                .config(PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST, properties.getProperty(PROPERTY_KEY_CASSANDRAN_CONNECTION_HOST));

        boolean isCassandraAnonymousLogin = Boolean.parseBoolean(properties.getProperty(PROPERTY_KEY_CASSANDRAN_ANONYMOUS_LOGIN));
        if(!isCassandraAnonymousLogin){
            builder.config(PROPERTY_KEY_CASSANDRAN_AUTH_USERNAME, properties.getProperty(PROPERTY_KEY_CASSANDRAN_AUTH_USERNAME));
            builder.config(PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD, properties.getProperty(PROPERTY_KEY_CASSANDRAN_AUTH_PASSWORD));
        }

        return builder.getOrCreate();
    }

}
