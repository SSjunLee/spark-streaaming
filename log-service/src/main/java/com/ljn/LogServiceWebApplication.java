package com.ljn;

import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class LogServiceWebApplication implements CommandLineRunner {
    public static void main(String args[]){
        SpringApplication.run(LogServiceWebApplication.class,args);
    }

    @Override
    public void run(String... args) throws Exception {
        if(args.length != 6){
            System.out.println("<usage> <file-path><ip><port><topic><interval><seconds>");
            System.exit(-1);
        }
        String dataFileName = args[0],ip=args[1];
        int port = Integer.parseInt(args[2]);
        String topic = args[3];
        int interval = Integer.parseInt(args[4]);
        int seconds = Integer.parseInt(args[5]);
        MeassageGenerator.work(dataFileName,ip,port,topic, interval,seconds);
        //Thread.currentThread().join();
    }
}
