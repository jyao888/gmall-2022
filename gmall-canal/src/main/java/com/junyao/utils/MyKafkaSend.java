package com.junyao.utils;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author wjy
 * @create 2022-06-26 16:17
 */
public class MyKafkaSend {


    private static KafkaProducer kafkaProducer = null;

    public static void send(String topic, String msg) {
        if (kafkaProducer==null){
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String,String>(topic,msg));
}

    private static KafkaProducer createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String, String> producer = null;

        try {
            producer = new KafkaProducer<>(props);
        }catch (Exception e){
            e.printStackTrace();
        }
        return producer;
    }
}