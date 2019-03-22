package com.example.demo;

import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class AtMostOnceProducer {
	
	public static void main(String[] args) {
		Properties props = new Properties();
		props.put("bootstrap.servers", "localhost:9092");
		props.put("acks", "0");// 0或者1
		props.put("retries", 0);// 关闭重传,只要关闭重传，就只有一次发送消息的机会
		props.put("enable.idempotence", false);
		props.put("batch.size", 16384);
		props.put("linger.ms", 0);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

		Producer<String, String> producer = new KafkaProducer<>(props);
		for (int i = 0; i < 100; i++) {
			producer.send(new ProducerRecord<String, String>("test", Integer.toString(i), Integer.toString(i)));
		}

		producer.close();
	}
}
