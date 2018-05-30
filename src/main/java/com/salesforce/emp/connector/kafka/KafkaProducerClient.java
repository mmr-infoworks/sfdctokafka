package com.salesforce.emp.connector.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Properties;
import java.util.UUID;

public class KafkaProducerClient {
    private final static String TOPIC = "my-example-topic";
    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092,localhost:9093,localhost:9094";

    Producer<String,String> producer;

    public KafkaProducerClient() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

         producer = new KafkaProducer<String,String>(props);

    }

    public void  produce(String topicName,String key ,String value) {
        ProducerRecord<String,String > producerRecord = new ProducerRecord<String,String>(topicName,key,value);
        System.out.println(String.format("Going to produce %s %s to topic %s",key,value,topicName));
//        producer.send(producerRecord);



    }

    public void shutDown() {
        try {
            producer.flush();
            producer.close();
        } finally {

        }
    }

    public static void main(String[] args) {
        KafkaProducerClient producerClient = new KafkaProducerClient();

        int i = 0;
            while (true) {

                producerClient.produce("testtopic2", UUID.randomUUID().toString(),"message"+i++);
                try {
                    Thread.sleep(1000l);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
    }

}
