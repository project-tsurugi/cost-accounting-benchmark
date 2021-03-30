package com.example.nedo;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDate;
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

	// batch
	public static int batchExecuteType() {
		return getPropertyInt("batch.execute.type", 2);
	}

	public static int batchJdbcType() {
		return getPropertyInt("batch.jdbc.type", 1);
	}

	public static int batchParallelism() {
		return getPropertyInt("batch.parallelism", 0);
	}

	public static int DECIMAL_SCALE = getPropertyInt("decimal.scale", 20);

	// online
	public static int onlineJdbcType() {
		return getPropertyInt("online.jdbc.type", 1);
	}

	public static int onlineTaskRatio(String taskName) {
		return getPropertyInt("online.task.ratio." + taskName);
	}

	public static int onlineTaskSleepTime(String taskName) {
		return getPropertyInt("online.task.sleep." + taskName, 0);
	}

	// initial data
	public static int initJdbcType() {
		return getPropertyInt("init.jdbc.type", 1);
	}

	public static LocalDate initBatchDate() {
		return getPropertyDate("init.batch.date");
	}

	public static int initFactorySize() {
		return getPropertyInt("init.factory.size");
	}

	public static int initItemProductSize() {
		return getPropertyInt("init.item.product.size");
	}

	public static int initItemWorkSize() {
		return getPropertyInt("init.item.work.size");
	}

	public static int initItemMaterialSize() {
		return getPropertyInt("init.item.material.size");
	}

	public static int initItemManufacturingSize() {
		return getPropertyInt("init.item.manufacturing.size");
	}

	public static int initParallelism() {
		int defaultValue = Runtime.getRuntime().availableProcessors();
		return getPropertyInt("init.parallelism", defaultValue);
	}

	private static Properties properties;

	private static Properties getProperties(boolean requiredFile) {
		if (properties == null) {
			String s = System.getProperty("property");
			if (s == null) {
				if (requiredFile) {
					throw new RuntimeException("not found -Dproperty=property-file-path");
				} else {
					return new Properties();
				}
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

	private static String getProperty(String key, boolean requiredFile) {
		Properties p = getProperties(requiredFile);
		return p.getProperty(key);
	}

	private static String getProperty(String key) {
		String s = getProperty(key, true);
		if (s == null) {
			throw new RuntimeException("not found key'" + key + "' in property-file");
		}
		return s;
	}

	private static int getPropertyInt(String key, int defaultValue) {
		String s = getProperty(key, false);
		if (s == null) {
			return defaultValue;
		}
		return getPropertyInt(key, s);
	}

	private static int getPropertyInt(String key) {
		String s = getProperty(key);
		return getPropertyInt(key, s);
	}

	private static int getPropertyInt(String key, String value) {
		try {
			String s = value.trim().replaceAll("_", "");
			return Integer.parseInt(s);
		} catch (NumberFormatException e) {
			throw new RuntimeException("not integer key'" + key + "' in property-file", e);
		}
	}

	private static LocalDate getPropertyDate(String key) {
		try {
			String s = getProperty(key).trim();
			return LocalDate.parse(s);
		} catch (NumberFormatException e) {
			throw new RuntimeException("not date key'" + key + "' in property-file", e);
		}
	}
}
