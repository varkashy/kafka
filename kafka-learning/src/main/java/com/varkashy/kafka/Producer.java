package com.varkashy.kafka;

import com.varkashy.kafka.util.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

public class Producer {

    public static void main(String[] args) {
        // Create Producer properties
        Properties kafkaProperties = ConfigUtil.getProperties();

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
