package com.hzsun.zbp.order.util;

import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.producer.ProducerRecord;

import javax.annotation.Nullable;

public class StringSerialization implements KafkaSerializationSchema<String> {


	private String topic;
	public StringSerialization(String s){
		this.topic = s;
	}

	@Override
	public ProducerRecord<byte[], byte[]> serialize(String o, @Nullable Long aLong) {
		return new ProducerRecord<>(topic,o.getBytes());
	}


}