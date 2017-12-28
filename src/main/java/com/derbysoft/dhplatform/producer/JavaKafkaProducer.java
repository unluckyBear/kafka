package com.derbysoft.dhplatform.producer;

import org.apache.kafka.clients.producer.*;

import java.util.Properties;
import java.util.concurrent.atomic.AtomicInteger;

public class JavaKafkaProducer {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "*****:9092");
        props.put("acks", "1");
        props.put("retries", 2);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        Producer<String, String> producer = new KafkaProducer<String, String>(props);

        try {
            System.out.println(producer.partitionsFor("hello"));
            final AtomicInteger success = new AtomicInteger(0);
            final AtomicInteger failed = new AtomicInteger(0);
            long start = System.currentTimeMillis();
            for (int i = 0; i < 3000; i++) {
                producer.send(new ProducerRecord<String, String>("hello", Integer.toString(i), "producer: test" + Integer.toString(i)), new Callback() {
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            System.out.println("Current failed count: " + failed.incrementAndGet());
                        } else {
                            System.out.println("Current success count: " + success.incrementAndGet() + ", failed: " + failed.get());
                        }
                    }
                });
            }
            long end = System.currentTimeMillis();
            System.out.println("producer spend time: " + (end - start) + "ms");
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            producer.close();
        }
    }
}
