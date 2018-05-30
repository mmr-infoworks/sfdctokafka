/* 
 * Copyright (c) 2016, salesforce.com, inc.
 * All rights reserved.
 * Licensed under the BSD 3-Clause license. 
 * For full license text, see LICENSE.TXT file in the repo root  or https://opensource.org/licenses/BSD-3-Clause
 */
package com.salesforce.emp.connector.example;

import static com.salesforce.emp.connector.LoginHelper.login;

import java.net.URL;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import com.salesforce.emp.connector.kafka.KafkaProducerClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.eclipse.jetty.util.ajax.JSON;

import com.salesforce.emp.connector.BayeuxParameters;
import com.salesforce.emp.connector.EmpConnector;
import com.salesforce.emp.connector.LoginHelper;
import com.salesforce.emp.connector.TopicSubscription;

/**
 * An example of using the EMP connector using login credentials
 *
 * @author hal.hildebrand
 * @since 202
 */
public class LoginExample {
    public static void main(String[] argv) throws Exception {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

        KafkaProducer<String,String> producer = new KafkaProducer<String,String>(props);
        if (argv.length < 3 || argv.length > 4) {
            System.err.println("Usage: LoginExample username password topic [replayFrom]");
            System.exit(1);
        }
        long replayFrom = EmpConnector.REPLAY_FROM_EARLIEST;
        if (argv.length == 4) {
            replayFrom = Long.parseLong(argv[3]);
        }

        BearerTokenProvider tokenProvider = new BearerTokenProvider(() -> {
            try {
                return login(argv[0], argv[1]);
            } catch (Exception e) {
                e.printStackTrace(System.err);
                System.exit(1);
                throw new RuntimeException(e);
            }
        });

        BayeuxParameters params = tokenProvider.login();
        KafkaProducerClient producerClient = new KafkaProducerClient();
        try {
            Consumer<Map<String, Object>> consumer = event -> {
                String eve = JSON.toString(event);
                ProducerRecord<String,String > producerRecord = new ProducerRecord<String,String>("testtopic",UUID.randomUUID().toString(),eve);
                System.out.println("going to produce  " + eve);
                producer.send(producerRecord);
                System.out.println("done  produce  " + eve);

            };

            EmpConnector connector = new EmpConnector(params);

            connector.setBearerTokenProvider(tokenProvider);

            connector.start().get(5, TimeUnit.SECONDS);

            TopicSubscription subscription = connector.subscribe(argv[2], replayFrom, consumer).get(5, TimeUnit.SECONDS);

            System.out.println(String.format("Subscribed: %s", subscription));
        } finally {
            if (producerClient!=null)  {
                producerClient.shutDown();
            }
        }
    }
}
