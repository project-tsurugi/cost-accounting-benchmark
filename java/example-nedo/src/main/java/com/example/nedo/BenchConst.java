package com.example.nedo;

public class BenchConst {

	public static final String JDBC_URL = "jdbc:postgresql://" + // DBURL
			"192.168.91.137" + // DBのあるIPアドレス（又はサーバー名）
			":" + "5432" + // DBが動いているポート
			"/" + "hdb"; // PostgreSQLのDB名
	public static final String JDBC_USER = "hishidama";
	public static final String JDBC_PASSWORD = "hishidama";

	public static final String DOC_DIR = "D:/cygwin/home/hishidama4/git/nautilus/tsurugi/cost-batch-benchmark/docs";
	public static final String TABLE_XLSX = DOC_DIR + "/table.xlsx";
	public static final String MEASUREMENT_XLSX = DOC_DIR + "/measurement.xlsx";
	public static final String SRC_DIR = "D:/cygwin/home/hishidama4/git/nautilus/tsurugi/cost-batch-benchmark/java/example-nedo/src/main/java";
	public static final String PACKAGE_DOMAIN = "com.example.nedo.jdbc.doma2.domain";
	public static final String PACKAGE_ENTITY = "com.example.nedo.jdbc.doma2.entity";

	public static int DECIMAL_SCALE = 20;
}
