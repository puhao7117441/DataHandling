package com.puhao.data.handling;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.context.ApplicationContext;
import org.springframework.context.annotation.AnnotationConfigApplicationContext;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.PropertySource;
import org.springframework.data.jpa.repository.config.EnableJpaRepositories;

import java.util.List;
import java.util.regex.Pattern;


/**
 * This is a standalone application that use Spring Data JPA to save data to MySQL database.
 * Since spring boot not in the dependency chain, you'll found pom have lot of dependencies which normally not appear if
 * spring boot is used.
 *
 * Created by pu on 2017/11/26.
 */
@Configuration
@ComponentScan
@EnableJpaRepositories(basePackages="com.puhao.data.handling")
@PropertySource("classpath:application.properties")
public class Main {

    private static Logger logger = LogManager.getLogger(Main.class);

    public static void main(String[] args){

        ApplicationContext context = new AnnotationConfigApplicationContext(Main.class);


        CSVReader reader = context.getBean(CSVReader.class);
        reader.readCSV(args);

        logger.info("Done");
    }
}
