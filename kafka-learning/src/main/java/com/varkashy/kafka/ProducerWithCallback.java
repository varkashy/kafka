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
            kafkaProducer.send(new ProducerRecord<String, String>("first-topic",
                    "hello world from java with index " + startIndex), new Callback() {
                public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                    //Executes everytime record is successfully sent or an exception is thrown
                    if(e!=null){
                        _log.error(e.getMessage());
                    }
                    else{
                        _log.info("Successfully sent "+recordMetadata.topic() +" - "+recordMetadata.partition()+" -"+
                                recordMetadata.offset()+" - "+recordMetadata.timestamp());
                    }
                }
            });
        }
        kafkaProducer.flush();

        kafkaProducer.close();

    }
}

