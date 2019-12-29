package com.varkashy.kafka;

import com.varkashy.kafka.util.ConfigUtil;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithKeys {

    public static final Logger _log = LoggerFactory.getLogger(ProducerWithKeys.class);
    public static void main(String[] args) {
        // Create Producer properties
        Properties kafkaProperties = ConfigUtil.getProperties();

        //Create Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

        //Send Data
        int keyIndex =-1;
        for (int startIndex = 0; startIndex < 10; startIndex++) {
            if(keyIndex==4){
                keyIndex=0;
            }
            else{
                keyIndex++;
            }
            ConfigUtil.produceMessage("fourth_topic",String.valueOf(keyIndex),"hello world from java with index " + startIndex,kafkaProducer);
        }
        kafkaProducer.flush();

        kafkaProducer.close();

    }
}

