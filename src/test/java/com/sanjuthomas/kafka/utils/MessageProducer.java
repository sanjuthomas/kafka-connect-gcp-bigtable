package com.sanjuthomas.kafka.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * Use this class to produce some sample message to test.
 * 
 * @author Sanju Thomas
 *
 */
public class MessageProducer {

  private static final ObjectMapper MAPPER = new ObjectMapper();

  public static void main(String[] args) {
    final Map<String, Object> messageMap = new HashMap<>();
    messageMap.put("client", "c-100");
    messageMap.put("exchange", "NYSE");
    messageMap.put("symbol", "FB");
    messageMap.put("price", 110.23);
    messageMap.put("quantity", 1200);
    Collections.nCopies(1000, 1)
    .stream()
    .forEach(i -> new MessageProducer().produceMessages(messageMap));
    
  }

  private void produceMessages(final Map<String, Object> message) {
    final Producer<String, JsonNode> producer =
        new KafkaProducer<String, JsonNode>(connectionProperties());
    final JsonNode messageNode = MAPPER.valueToTree(message);
    producer.send(new ProducerRecord<String, JsonNode>("demo-topic", messageNode));
    producer.close();
  }

  private Properties connectionProperties() {
    final Properties props = new Properties();
    props.put("bootstrap.servers", "localhost:9092");
    props.put("acks", "all");
    props.put("retries", 3);
    props.put("batch.size", 100);
    props.put("linger.ms", 1);
    props.put("buffer.memory", 2048);
    props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
    props.put("value.serializer", "org.apache.kafka.connect.json.JsonSerializer");
    return props;
  }

}
