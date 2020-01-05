package com.varkashy.kafka;


import com.varkashy.kafka.util.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

public class Consumer {

    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
    public static void main(String[] args){
        {
            String groupId = "fourth-topic-group-1";
            String topic = "fourth_topic";

            // create consumer configs
            Properties consumerProperties = ConfigUtil.getConsumerProperties(groupId,"earliest");

            // create consumerConsumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);

            // subscribe consumer to our topic(s)
            consumer.subscribe(Arrays.asList(topic));

            // poll for new data
            while(true){
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                for (ConsumerRecord<String, String> record : records){
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                }
            }

        }
    }
}
