package com.ljn;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.log4j.Logger;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class MeassageGenerator {

    private static final Logger logger  = Logger.getLogger(MeassageGenerator.class);
    static void  work(String path, String ip, int port, String topic, int interval,int dur) throws IOException {
        Properties props = new Properties();
        props.put("bootstrap.servers", ip+":"+port);
        props.put("acks", "all");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<>(props);
        BufferedReader reader = new BufferedReader(new FileReader(path));//换成你的文件名
        String line = null;
        int id = 0;
        while ((line = reader.readLine())!=null)
        {
            producer.send(new ProducerRecord<String,String>(topic,Integer.toString(id),line));
            id++;
            if(id%interval!=0)continue;
            try {
                Thread.sleep(dur* 1000L);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        producer.close();
        reader.close();
    }
}
