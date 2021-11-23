package com.ljn.ss.util;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class ConfigUtil {
   /* kafka.ip=vm
    kafka.port=9092
    kafka.topic=test

    mongo.ip=vm
    mongo.port=27017
    mongo.dbname=ljn
    */

    public final static String KAFKA_IP = "kafka.ip";
    public final static String KAFKA_PORT = "kafka.port";
    public final static String KAFKA_TOPIC = "kafka.topic";

    public final static String MONGO_IP = "mongo.ip";
    public final static String MONGO_PORT = "mongo.port";
    public final static String MONGO_DBNAME = "mongo.dbname";

    public final static String ENV = "env";
    public final static String INTERVAL ="interval";



    private Properties properties;
    public ConfigUtil(String path) throws IOException {
        properties = new Properties();
        BufferedReader bf = new BufferedReader(new FileReader(path));
        properties.load(bf);
    }
    public String get(String key){
        return properties.getProperty(key);
    }

   /* public static void main(String args[]) throws IOException {
        ConfigUtil configUtil =
                new ConfigUtil("D:\\code\\java\\pkspark\\log-service\\src\\main\\resources\\log4j.properties");
        System.out.println(configUtil.get("log4j.appender.console.layout"));
    }

    */
}
