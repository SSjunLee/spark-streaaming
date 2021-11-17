package util;

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

import java.io.*;
import java.util.ArrayList;
import java.util.Properties;




public class KafkaProducer extends Thread{

    private final String topic = KafkaProperties.TOPIC;
    static String path = "C:\\Users\\12206\\Desktop\\test.csv";

    private Producer<Integer, String> getProducer(){

        Properties properties = new Properties();
        properties.put("metadata.broker.list",KafkaProperties.BROKER_LIST);
        properties.put("serializer.class","kafka.serializer.StringEncoder");
        properties.put("request.required.acks","1");

        return new Producer<Integer, String>(new ProducerConfig(properties));
    }


    public ArrayList<String> getInfo(){
        ArrayList<String> res = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(path));//换成你的文件名
            reader.readLine();//第一行信息，为标题信息，不用,如果需要，注释掉
            String line = null;
            while((line=reader.readLine())!=null){
                res.add(line);
                //String item[] = line.split(",");//CSV格式文件为逗号分隔符文件，这里根据逗号切分
            }
            reader.close();
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
        return  res;
    }



    @Override
    public void run() {
        ArrayList<String> fileInfo = getInfo();
        Producer producer = getProducer();
        int i = 0;
        while(true) {
            String message = fileInfo.get(i);
            producer.send(new KeyedMessage<Integer, String>(topic, message));
            i++;
            if(i%20==0){
                try{
                    Thread.sleep(3000);
                } catch (Exception e){
                    e.printStackTrace();
                }
            }
            if(i == fileInfo.size())i=0;
        }
    }
}
