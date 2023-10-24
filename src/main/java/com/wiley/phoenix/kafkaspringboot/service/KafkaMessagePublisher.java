package com.wiley.phoenix.kafkaspringboot.service;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaMessagePublisher {


    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Value("${app.kafka.topic.partitioned}")
    private String kafkaPartitionedTopic;

    public void sendMessageDefaultTopic(String message) {
        CompletableFuture<SendResult<String, Object>> futureResult = kafkaTemplate.sendDefault(message); // This will send to the default kafka topic defined in the property file
//        futureResult.get(); // This will slow down the producer. So better to handle messages asynchronously as below
        handleMessaging(message, futureResult);
    }

    public void sendMessageCustomTopic(String kafkaTopic, String message) {
        CompletableFuture<SendResult<String, Object>> futureResult = kafkaTemplate.send(kafkaTopic, message); // This will send to the exact kafka topic provided here it will auto create the topic for you if it is not already created
        handleMessaging(message, futureResult);
    }


    public void sendMultipleMessageTopic(String[] messages) {
        for (String model : messages) {
            sendMessageCustomTopic(kafkaPartitionedTopic, model);
        }
    }

    private static void handleMessaging(String message, CompletableFuture<SendResult<String, Object>> futureResult) {
        futureResult.whenComplete((result, ex) -> {
            if (null == ex) {
                System.out.println("Sent message=[" + message + "] with offset=[" + result.getRecordMetadata().offset() + "] with partition=[" + result.getRecordMetadata().partition() + "]");
            } else {
                System.out.println("Unable to send message=[" + message + "] due to: " + ex.getMessage());
            }
        });
    }
}
