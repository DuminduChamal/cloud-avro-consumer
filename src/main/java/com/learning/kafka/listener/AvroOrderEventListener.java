package com.learning.kafka.listener;

import com.learning.kafka.dto.OrderEventAvro;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

@Component
public class AvroOrderEventListener {

    @KafkaListener(topics = "cloud-avro-orders-topic")
    public void listen(ConsumerRecord<String, OrderEventAvro> record) {
        OrderEventAvro order = record.value();
        System.out.printf("partition=%d offset=%d key=%s order=%s%n",
                record.partition(), record.offset(), record.key(), order);

    }
}