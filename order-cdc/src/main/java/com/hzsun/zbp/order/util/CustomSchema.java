package com.hzsun.zbp.order.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Schema;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CustomSchema implements DebeziumDeserializationSchema<String> {
	@Override
	public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {


		// 1 init
		//获取 database和 table
        String topic = sourceRecord.topic();
        //分隔符得写 \\. 不然就报错
        String[] strings = topic.split("\\.");
        String database = strings[1];
        String table = strings[2];
		//String s = sourceRecord.value().toString();
		Struct value = (Struct) sourceRecord.value();

		// 2 before
		SimpleDateFormat sdf1 = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
		SimpleDateFormat sdf2 = new SimpleDateFormat("yyyy-MM-dd");
		Struct before = value.getStruct("before");
		HashMap<String, Object> beforeData = new HashMap<>();
		TimeTrans(sdf2, beforeData, before);

		// 3 after
        Struct after = value.getStruct("after");
        HashMap<String, Object> afterData = new HashMap<>();
		TimeTrans(sdf2, afterData, after);

		Envelope.Operation op = Envelope.operationFor(sourceRecord);
		String stringOp = op.toString().toLowerCase();
		if ("create".equals(stringOp)){
			stringOp = "insert";
		}

		HashMap<Object, Object> schame = new HashMap<>();
		// 1 tableName
		schame.put("operation",stringOp);
		schame.put("tableName", table);
		// 2 before
		schame.put("before",beforeData);
		// 3 after
		schame.put("after",afterData);
		// 4  op

		ObjectMapper object = new ObjectMapper();

		//object.writeValue("231",database);
		//collector.collect(object1.writeValueAsString(schame));
		collector.collect(object.writeValueAsString(schame));


	}

	private void TimeTrans(SimpleDateFormat sdf2, HashMap<String, Object> hashMap, Struct structs) {
		if (structs != null){
			for (Field field : structs.schema().fields()) {
				//afterData.put(field.name(), o);
				Object afterValue = structs.get(field);
				if ("int32".equals(field.schema().type().getName())&&"io.debezium.time.Date".equals(field.schema().name()) ){
					if (afterValue != null){
						int times = (int) afterValue;
						String format = sdf2.format(new Date(times * 24 * 60 * 60L * 1000));
						//beforeValue
						hashMap.put(field.name(), format);
					}
				}else{
					hashMap.put(field.name(), afterValue);
				}

			}
		}
	}

	@Override
	public TypeInformation getProducedType() {
		return BasicTypeInfo.STRING_TYPE_INFO;
	}
}

// latest analysis
