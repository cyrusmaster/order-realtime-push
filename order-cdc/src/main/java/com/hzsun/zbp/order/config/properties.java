package com.hzsun.zbp.order.config;



import java.io.InputStream;

public class properties {

	public static InputStream getInputStream(){
		return properties.class.getClassLoader().getResourceAsStream("application.properties");

	}

	//public static InputStream inputStream = properties.class.getClassLoader().getResourceAsStream("application.properties");
	//
	//
	//public static InputStream resourceAsStream = properties.class.getClassLoader().getResourceAsStream("application.properties");




}
