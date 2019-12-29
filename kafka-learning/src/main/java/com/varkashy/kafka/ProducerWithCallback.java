package com.varkashy.kafka;

import com.varkashy.kafka.util.ConfigUtil;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerWithCallback {

    public static final Logger _log = LoggerFactory.getLogger(ProducerWithCallback.class);
    public static void main(String[] args) {
        // Create Producer properties
        Properties kafkaProperties = ConfigUtil.getProperties();

        //Create Producer
        KafkaProducer<String, String> kafkaProducer = new KafkaProducer<String, String>(kafkaProperties);

        //Send Data
        for (int startIndex = 0; startIndex < 5; startIndex++) {
            ConfigUtil.produceMessage("first-topic",null,"hello world from java with index " + startIndex,kafkaProducer);
        }
        kafkaProducer.flush();

        kafkaProducer.close();

    }
}

