package com.example.nedo;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Properties;

public class BenchConst {

	public static String jdbcUrl() {
		return getProperty("jdbc.url");
	}

	public static String jdbcUser() {
		return getProperty("jdbc.user");
	}

	public static String jdbcPassword() {
		return getProperty("jdbc.password");
	}

	public static String docDir() {
		return getProperty("doc.dir");
	}

	public static String tableXlsxPath() {
		return docDir() + "/table.xlsx";
	}

	public static String measurementXlsxPath() {
		return docDir() + "/measurement.xlsx";
	}

	public static String srcDir() {
		return getProperty("src.dir");
	}

	public static final String PACKAGE_DOMAIN = "com.example.nedo.jdbc.doma2.domain";
	public static final String PACKAGE_ENTITY = "com.example.nedo.jdbc.doma2.entity";

	public static int DECIMAL_SCALE = 20;

	private static Properties properties;

	private static Properties getProperties() {
		if (properties == null) {
			String s = System.getProperty("property");
			if (s == null) {
				throw new RuntimeException("not found -Dproperty=property-file-path");
			}
			Path path = Paths.get(s);
			try (BufferedReader reader = Files.newBufferedReader(path, StandardCharsets.UTF_8)) {
				Properties p = new Properties();
				p.load(reader);
				properties = p;
			} catch (IOException e) {
				throw new RuntimeException(e);
			}
		}
		return properties;
	}

	private static String getProperty(String key) {
		Properties p = getProperties();
		String s = p.getProperty(key);
		if (s == null) {
			throw new RuntimeException("not found key'" + key + "' in property-file");
		}
		return s;
	}
}
