package com.example.nedo.db;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

public class DBUtils {
	private static  DateFormat DF = new SimpleDateFormat("yyyy-MM-dd");

	public static Connection getConnection() {
		// TODO Configで指定可能にする
        String url = "jdbc:postgresql://127.0.0.1/umegane";
        String user = "umegane";
        String password = "umegane";
        Connection conn;
		try {
			conn = DriverManager.getConnection(url, user, password);
			conn.setAutoCommit(false);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return conn;
	}

	public static Date toDate(String date) {
		try {
			return new Date(DF.parse(date).getTime());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

}
