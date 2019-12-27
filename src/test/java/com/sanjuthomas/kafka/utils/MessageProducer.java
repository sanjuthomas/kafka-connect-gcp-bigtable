/*
 * Copyright (c) 2019 Sanju Thomas
 *
 * Licensed under the MIT License (the "License");
 * you may not use this file except in compliance with the License.
 *
 * You may obtain a copy of the License at https://en.wikipedia.org/wiki/MIT_License
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied.  See the License for the specific language governing
 * permissions and limitations under the License.
 *
 */

package com.sanjuthomas.kafka.utils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.UUID;
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
    Collections.nCopies(1, 1)
    .stream()
    .forEach(i -> new MessageProducer().produceMessagesWithKey(messageMap));
    Collections.nCopies(1, 1)
    .stream()
    .forEach(i -> new MessageProducer().produceMessages(messageMap));
  }

  private void produceMessagesWithKey(final Map<String, Object> message) {
    final Producer<String, JsonNode> producer =
        new KafkaProducer<String, JsonNode>(connectionProperties());
    final JsonNode messageNode = MAPPER.valueToTree(message);
    producer.send(new ProducerRecord<String, JsonNode>("test-topic", UUID.randomUUID().toString(), messageNode));
    producer.close();
  }

  private void produceMessages(final Map<String, Object> message) {
    final Producer<String, JsonNode> producer =
        new KafkaProducer<String, JsonNode>(connectionProperties());
    final JsonNode messageNode = MAPPER.valueToTree(message);
    producer.send(new ProducerRecord<String, JsonNode>("demo-topic",  messageNode));
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
