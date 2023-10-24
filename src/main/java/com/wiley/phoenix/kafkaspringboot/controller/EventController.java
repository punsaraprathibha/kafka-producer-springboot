package com.wiley.phoenix.kafkaspringboot.controller;


import com.wiley.phoenix.kafkaspringboot.service.KafkaMessagePublisher;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/kafka-producer")
public class EventController {

    @Autowired
    private KafkaMessagePublisher publisher;


    @PostMapping("/publish/default")
    public ResponseEntity<Object> publishMessageThroughDefaultTopic(@RequestParam(name = "message") String message) {
        try {
            publisher.sendMessageDefaultTopic(message);
            return ResponseEntity.ok("Message Published Successfully through default topic!");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Message was unable to publish!");
        }
    }

    @PostMapping("/publish/custom")
    public ResponseEntity<Object> publishMessageThroughCustomTopic(@RequestParam(name = "topic") String topicName,
                                                                   @RequestParam(name = "message") String message) {
        try {
            publisher.sendMessageCustomTopic(topicName, message);
            return ResponseEntity.ok("Message Published Successfully to custom topic!");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Message was unable to publish!");
        }
    }

    @PostMapping("/publish/multiple")
    public ResponseEntity<Object> publishMessageThroughPartitionedTopic(@RequestBody String[] messages) {
        try {
            publisher.sendMultipleMessageTopic(messages);
            return ResponseEntity.ok("Multiple Message Published Successfully!");
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body("Message was unable to publish!");
        }
    }
}
