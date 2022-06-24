package com.junyao.utils;

import com.alibaba.fastjson.JSONObject;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;

/**
 * @author wjy
 * @create 2022-06-23 18:02
 */
public class MyKafkaSender {

    private static KafkaProducer<String, String> kafkaProducer = null;

    public static void send(String topic,String msg) {
        if (kafkaProducer==null){
            kafkaProducer = createKafkaProducer();
        }
        kafkaProducer.send(new ProducerRecord<String,String>(topic,msg));
    }

    private static KafkaProducer<String, String> createKafkaProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "hadoop102:9092,hadoop103:9092,hadoop104:9092");
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        KafkaProducer<String, String> producer=null;
        try {
            producer = new KafkaProducer<>(props);
        }catch (Exception e){
            e.printStackTrace();
        }
        return producer;
    }

}
