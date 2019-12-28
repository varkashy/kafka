package com.varkashy.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        // Create Producer properties
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());

        //Create Producer
        KafkaProducer<String,String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

        //Send Data
        for(int startIndex=0;startIndex<5;startIndex++){
            System.out.println("Sending data");
            kafkaProducer.send(new ProducerRecord<String, String>("first-topic","hello world from java with index "+startIndex));
        }
        kafkaProducer.flush();

        kafkaProducer.close();

    }
}
