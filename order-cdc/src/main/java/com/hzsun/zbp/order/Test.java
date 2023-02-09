package com.hzsun.zbp.order;


import com.hzsun.zbp.order.config.properties;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;


import java.io.IOException;

public class Test {
	public static void main(String[] args) throws Exception {

	ParameterTool parameterTool = ParameterTool.fromPropertiesFile(properties.getInputStream());
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);
		tableEnv.getConfig().getConfiguration().setString("table.exec.sink.not-null-enforcer","drop");
        //tableEnv.executeSql("drop table products_mys_cdc");
        String strSql = "  CREATE TABLE products_my_cdc (\n" +
                "    stu_no STRING,\n" +
                "    stu_name STRING ,\n" +
                "    sex_code int ,\n" +
				//"    staff_no STRING,\n" +
                //"    staff_name STRING ,\n" +
                //"    bd_date DATE ,\n" +
                "    PRIMARY KEY (stu_no) NOT ENFORCED\n" +
                "  ) WITH (\n" +
                "    'connector' = 'mysql-cdc',\n" +
                "    'hostname' = '" + parameterTool.get("hostname") + "',\n" +
                "    'port' = '" + parameterTool.get("port") + "',\n" +
                "    'username' = '" + parameterTool.get("username") + "',\n" +
                "    'password' = '" + parameterTool.get("password") + "',\n" +
                "    'database-name' = '" + parameterTool.get("databaseList") + "',\n" +
                "    'table-name' = '"+ parameterTool.get("tableList") +"' ,\n" +
                "    'debezium.log.mining.continuous.mine'='true',\n"+
                "    'debezium.log.mining.strategy'='online_catalog',\n" +
                //"    'debezium.database.tablename.case.insensitive'='false',\n"+
                "    'scan.startup.mode' = 'latest-offset'\n"+
                //"    'scan.startup.mode' = 'initial'\n"+
                //"    'table.exec.sink.not-null-enforcer' = 'drop',\n"+
                //"    'scan.incremental.snapshot.enabled' = 'false'" +
                 ")";


		tableEnv.executeSql(strSql);


		//tableEnv.executeSql("  CREATE TABLE result (\n" +
		//
        //        "    res int \n" +
		//		//"    staff_no STRING,\n" +
        //        //"    staff_name STRING ,\n" +
        //        //"    bd_date DATE ,\n" +
		//
        //        "  ) WITH (\n" +
		//
        //         ")");

		Table table = tableEnv.sqlQuery("select sum(sex_code) as sumr from products_my_cdc");


		tableEnv.toRetractStream(table, Model.class).print();

		//DataStream<Tuple2<Boolean, Row>> tuple2DataStream = tableEnv.toRetractStream(table, Row.class);
		//tuple2DataStream.filter(new FilterFunction<Tuple2<Boolean, Row>>() {
		//	@Override
		//	public boolean filter(Tuple2<Boolean, Row> booleanRowTuple2) throws Exception {
		//		System.out.println(booleanRowTuple2.f1);
		//		//if (String.valueOf(booleanRowTuple2.f1).contains("I")){
		//		//	System.out.println("ok");
		//		//}
		//
		//		//System.out.println(booleanRowTuple2.f1.getArity());
		//		System.out.println(booleanRowTuple2.f1.getKind());
		//
		//		return true;
		//	}
		//});
		//tableEnv.toChangelogStream(table).print();

		env.execute();

	}



}
