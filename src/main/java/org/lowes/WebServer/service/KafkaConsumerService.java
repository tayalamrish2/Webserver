package org.lowes.WebServer.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lowes.example.Product;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
public class KafkaConsumerService {

    @Autowired
    private SimpMessagingTemplate messagingTemplate;

    @Value("${app.websocket.topic}")
    private String webSocketTopic;
    private final ObjectMapper objectMapper = new ObjectMapper();
    @KafkaListener(topics = "${app.kafka.product.topic}", groupId = "${spring.kafka.consumer.group-id}")
    public void listen(String message) {
        try {
            Product prod = objectMapper.readValue(message, Product.class);
            System.out.println("webSocketTopic: " + webSocketTopic);
            messagingTemplate.convertAndSend(webSocketTopic, message);
            System.out.println("Published: " + message);
        } catch (Exception e) {
            System.out.println("Invalid message received: " + message);
            e.printStackTrace();
        }
    }
}
