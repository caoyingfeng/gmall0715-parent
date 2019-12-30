package com.cy.gmall0715.canal.util;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
 * @author cy
 * @create 2019-12-30 18:36
 */
public class KafkaSender {

    public static KafkaProducer<String,String> kafkaProducer=null;

    public static KafkaProducer<String,String> createKafkaProducer(){
        Properties config = null;
        try {
            config = PropertiesUtil.load("config.properties");
        } catch (IOException e) {
            e.printStackTrace();
        }
        String broker_list = config.getProperty("kafka.broker.list");

        Properties properties = new Properties();
        properties.put("bootstrap.servers",broker_list);
        properties.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        properties.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer = null;
        producer = new KafkaProducer<>(properties);

        return producer;
    }
    public static void send(String topic,String msg){
        if(kafkaProducer == null){
            kafkaProducer=createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String,String>(topic,msg));
    }
}
