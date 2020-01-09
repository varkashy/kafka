package com.varkashy.kafka;


import com.varkashy.kafka.util.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerAssignAndSeek {

    private static final Logger logger = LoggerFactory.getLogger(ConsumerAssignAndSeek.class);
    public static void main(String[] args){
        {
            // create consumer configs
            Properties consumerProperties = ConfigUtil.getConsumerProperties("earliest");

            // create consumerConsumer
            KafkaConsumer<String, String> consumer = new KafkaConsumer<String, String>(consumerProperties);

            //Assign and seek are used to replay data or get specific message

            TopicPartition partitionToReadFrom = new TopicPartition("fourth_topic",0);
            long offsetToReadFrom = 2L;
            consumer.assign(Arrays.asList(partitionToReadFrom));
            consumer.seek(partitionToReadFrom,offsetToReadFrom);

            int countOfMessages = 0;
            boolean keepOnReading = true;
            // poll for new data
            while(keepOnReading){
                ConsumerRecords<String, String> records =
                        consumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                for (ConsumerRecord<String, String> record : records){
                    countOfMessages++;
                    logger.info("Key: " + record.key() + ", Value: " + record.value());
                    logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    if(countOfMessages>=5){
                        keepOnReading = false;
                        break;
                    }
                }
            }

        }
    }
}
