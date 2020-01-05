package com.varkashy.kafka.util;

import com.varkashy.kafka.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ConfigUtil<K,V> {

    public static final Logger _log = LoggerFactory.getLogger(ConfigUtil.class);

    public static Properties getProperties() {
        Properties kafkaProperties = new Properties();
        kafkaProperties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,"127.0.0.1:9092");
        kafkaProperties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        kafkaProperties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG,StringSerializer.class.getName());
        return kafkaProperties;
    }

    public static <K,V> void produceMessage (String topic, K key, V message, KafkaProducer<K,V> kafkaProducer ){
        ProducerRecord<K,V> producerRecord ;

        if(key!=null){
            producerRecord = new ProducerRecord<K, V>(topic,key,message);
        }
        else{
            producerRecord = new ProducerRecord<K, V>(topic,message);
        }
        kafkaProducer.send(producerRecord, new Callback() {
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

    public static Properties getConsumerProperties(String groupId, String offsetConfig) {
        Properties consumerProperties = new Properties();
        consumerProperties.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,"localhost:9092");
        consumerProperties.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,StringDeserializer.class.getName());
        consumerProperties.setProperty(ConsumerConfig.GROUP_ID_CONFIG,groupId);
        consumerProperties.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG,offsetConfig);
        return  consumerProperties;
    }
}
