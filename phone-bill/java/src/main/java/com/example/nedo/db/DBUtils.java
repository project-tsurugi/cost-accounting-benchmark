package com.example.nedo.db;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class DBUtils {

	public static Connection getConnection() {
		// TODO Configで指定可能にする
        String url = "jdbc:postgresql://127.0.0.1/postgres";
        String user = "umegane";
        String password = "umegane";
        Connection conn;
		try {
			conn = DriverManager.getConnection(url, user, password);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
		return conn;
	}
}
