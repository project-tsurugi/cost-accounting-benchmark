package com.example.nedo.db;

import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;

import com.example.nedo.app.Config;

public class DBUtils {
	/**
	 * ミリ秒で表した1日
	 */
	public static final long A_DAY_IN_MILLISECONDS = 24 * 3600 * 1000;

	private static  DateFormat DF_DATE = new SimpleDateFormat("yyyy-MM-dd");
	private static  DateFormat DF_TIMESTAMP = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");

	public static Connection getConnection(Config config) {
        Connection conn;
		try {
			conn = DriverManager.getConnection(config.url, config.user, config.password);
			conn.setAutoCommit(false);
			conn.setTransactionIsolation(Connection.TRANSACTION_READ_COMMITTED);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return conn;
	}

	public static Date toDate(String date) {
		try {
			return new Date(DF_DATE.parse(date).getTime());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	public static Timestamp toTimestamp(String date) {
		try {
			return new Timestamp(DF_TIMESTAMP.parse(date).getTime());
		} catch (ParseException e) {
			throw new RuntimeException(e);
		}
	}

	/**
	 * 指定のdateの次の日を返す
	 *
	 * @param date
	 * @return
	 */
	public static Date nextDate(Date date) {
		return new Date(date.getTime() + DBUtils.A_DAY_IN_MILLISECONDS);
	}

}
