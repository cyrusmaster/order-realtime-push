package com.hzsun.zbp.order;


import com.hzsun.zbp.order.config.properties;
import com.hzsun.zbp.order.sink.Kafka;
import com.hzsun.zbp.order.util.CustomSchema;
import com.ververica.cdc.connectors.mysql.source.MySqlSource;
import com.ververica.cdc.connectors.mysql.table.StartupOptions;
import com.ververica.cdc.debezium.DebeziumDeserializationSchema;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;
import com.ververica.cdc.debezium.table.RowDataDebeziumDeserializeSchema;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.aggregation.AggregationFunction;
import org.apache.flink.streaming.api.windowing.assigners.SlidingProcessingTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.streaming.api.windowing.triggers.Trigger;
import org.apache.flink.streaming.api.windowing.triggers.TriggerResult;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;

import java.text.SimpleDateFormat;
import java.util.Date;


public class OrderETL {

	public static void main(String[] args) throws Exception {


		ParameterTool parameterTool = ParameterTool.fromPropertiesFile(properties.getInputStream());
		MySqlSource<String> build = MySqlSource.<String>builder()
				.hostname(parameterTool.get("hostname"))
				.port(parameterTool.getInt("port"))
				.databaseList(parameterTool.get("databaseList"))
				.tableList(parameterTool.get("tableList"))
				.username(parameterTool.get("username"))
				.password(parameterTool.get("password"))
				.startupOptions(StartupOptions.latest())
				.deserializer(new CustomSchema())
				//.deserializer((DebeziumDeserializationSchema<String>) new RowDataDebeziumDeserializeSchema.Builder())
				.build();



		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		DataStreamSource<String> mysql = env.fromSource(build, WatermarkStrategy.noWatermarks(), "mysql");
		// sliding    20h  3-23
		//mysql.print();

		SingleOutputStreamOperator<String> reduce = mysql.windowAll(SlidingProcessingTimeWindows.of(Time.hours(20), Time.days(1), Time.hours(-5)))

				.trigger(new Trigger<String, TimeWindow>() {
					@Override
					public TriggerResult onElement(String s, long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
						SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
						Date date = new Date(timeWindow.getStart());
						Date date1 = new Date(timeWindow.getEnd());
						System.out.println(simpleDateFormat.format(date) + "---" + simpleDateFormat.format(date1));
						return TriggerResult.FIRE;
					}

					@Override
					public TriggerResult onProcessingTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
						return TriggerResult.CONTINUE;
					}

					@Override
					public TriggerResult onEventTime(long l, TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {
						return null;
					}

					@Override
					public void clear(TimeWindow timeWindow, TriggerContext triggerContext) throws Exception {

					}
				})
				.reduce(new AggregationFunction<String>() {
					@Override
					public String reduce(String s, String t1) throws Exception {
						return t1;
					}
				});
		reduce.print();
		reduce.addSink(Kafka.getSingleConsumeSink());
		env.execute("etl");

	}

}


 //  消费分析  后端