package com.wiley.phoenix.kafkaspringboot.configuration;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
public class KafkaProducerConfig {

    @Value("${app.kafka.topic.partitioned}")
    private String kafkaPartitionedTopic;
    @Value("${app.kafka.topic.partition-count}")
    private int noOfPartitions;
    @Value("${app.kafka.topic.replication-factor}")
    private int replicationFactor;

    @Bean
    public NewTopic createTopic() {
        return new NewTopic(kafkaPartitionedTopic, noOfPartitions, (short) replicationFactor);
    }
}
