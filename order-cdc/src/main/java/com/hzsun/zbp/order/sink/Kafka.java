package com.hzsun.zbp.order.sink;

import com.hzsun.zbp.order.config.properties;
import com.hzsun.zbp.order.util.StringSerialization;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import java.util.Properties;

public class Kafka {

	public static FlinkKafkaProducer<String> getSingleConsumeSink() throws Exception {



		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(properties.getInputStream());
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers",parameterTool.get("servers"));
			return new FlinkKafkaProducer<String>(parameterTool.get("topic"), new StringSerialization(parameterTool.get("topic")), properties, FlinkKafkaProducer.Semantic.EXACTLY_ONCE);
		}




}
