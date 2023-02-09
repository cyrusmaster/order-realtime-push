package com.hzsun.zbp.order.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.mysql.cj.result.Row;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import io.debezium.data.Envelope;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.util.Collector;
import org.apache.kafka.connect.data.Field;
import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;

import java.util.HashMap;

public class TestCustomSchema implements DebeziumDeserializationSchema<Row> {
	@Override
	public void deserialize(SourceRecord sourceRecord, Collector collector) throws Exception {


		//获取 database和 table
        String topic = sourceRecord.topic();
        //分隔符得写 \\. 不然就报错
        String[] strings = topic.split("\\.");
        String database = strings[1];
        String table = strings[2];

		//String s = sourceRecord.value().toString();
		Struct value = (Struct) sourceRecord.value();
		Struct before = value.getStruct("before");
		HashMap<String, Object> beforeData = new HashMap<>();
		if (before != null){
			for (Field field : before.schema().fields()) {
				Object o = before.get(field);
				beforeData.put(field.name(), o);
			}
		}

        Struct after = value.getStruct("after");
        HashMap<String, Object> afterData = new HashMap<>();
        if (after != null){
			for (Field field : after.schema().fields()) {
				Object o = after.get(field);
				afterData.put(field.name(), o);
			}
        }

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

	@Override
	public TypeInformation getProducedType() {
		return BasicTypeInfo.STRING_TYPE_INFO;
	}
}

// latest analysis
