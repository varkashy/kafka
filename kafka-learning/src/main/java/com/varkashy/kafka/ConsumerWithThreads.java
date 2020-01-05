package com.varkashy.kafka;


import com.varkashy.kafka.util.ConfigUtil;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

public class ConsumerWithThreads {

    public static AtomicInteger count = new AtomicInteger(1);
    private static final Logger logger = LoggerFactory.getLogger(ConsumerWithThreads.class);
    public static void main(String[] args){
        {
            CountDownLatch countDownLatch = new CountDownLatch(1);
            Runnable consumerRunnable = new ConsumerThread(countDownLatch,"fourth_topic",
                    "fourth-topic-group-threads-1","earliest");
            Thread consumerThread = new Thread(consumerRunnable);
            consumerThread.start();
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                logger.info("caught shutdown hook");
                ((ConsumerThread)consumerRunnable).shutdown();
                try {
                    countDownLatch.await();
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }));
            try {
                countDownLatch.await();
            } catch (InterruptedException e) {
                logger.info("Application interrupted "+e);
            }finally {
                logger.info("Application Closing");
            }
        }
    }

    public static class ConsumerThread<K,V> implements Runnable{

        private CountDownLatch countDownLatch;
        KafkaConsumer<K, V> kafkaConsumer;

        public ConsumerThread(CountDownLatch latch,String topic,String groupId,String offset){
            this.countDownLatch = latch;
            this.kafkaConsumer = new KafkaConsumer<K, V>(ConfigUtil.getConsumerProperties(groupId,offset));
            kafkaConsumer.subscribe(Arrays.asList(topic));
        }
        @Override
        public void run() {
            // poll for new data
            try{
                while(true){
                    ConsumerRecords<K, V> records =
                            kafkaConsumer.poll(Duration.ofMillis(100)); // new in Kafka 2.0.0

                    for (ConsumerRecord<K, V> record : records){
                        logger.info("Key: " + record.key() + ", Value: " + record.value());
                        logger.info("Partition: " + record.partition() + ", Offset:" + record.offset());
                    }
                }
            }
            catch(WakeupException wakeUpException){
                logger.info("Received shutdown signal");
            } finally {
                kafkaConsumer.close();
                countDownLatch.countDown();
            }
        }

        public void shutdown(){
            /*
            This interrupts consumer.poll which will throw a wake up exception
             */
            kafkaConsumer.wakeup();
        }
    }
}


