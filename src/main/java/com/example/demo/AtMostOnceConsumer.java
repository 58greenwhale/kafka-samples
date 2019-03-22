package com.example.demo;

import java.time.Duration;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

/**
 * 记录的是某个topic，某个分区的偏移量，topic name,partition number;<br>
 * id，topicname，partition number，offset<br>
 * 
 * @author chenzhenyang
 *
 */
public class AtMostOnceConsumer {
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("group.id", "test");
		props.put("enable.auto.commit", "false");
		props.put("isolation.level", "read_uncommitted");
		// props.put("auto.commit.interval.ms", "1000");
		props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
		@SuppressWarnings("resource")
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
		consumer.subscribe(Arrays.asList("test"));
		while (true) {
			ConsumerRecords<String, String> records = consumer.poll(Duration.of(1L, ChronoUnit.SECONDS));
			for (ConsumerRecord<String, String> record : records) {
				System.out.printf("==================offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
			}
		}
	}
}
