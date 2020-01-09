package com.varkashy.kafka.util;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class PropertyUtil {
	public static Properties properties ;
	static{
		InputStream inputStream = PropertyUtil.class.getResourceAsStream("/config.properties");
		properties = new Properties();
		try {
			properties.load(inputStream);
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
