package com.example.nedo;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;

import com.example.nedo.app.Config;
import com.example.nedo.db.DBUtils;

/**
 * DBにアクセスするテストケース共通用のクラス。
 *
 */
public abstract class AbstractDbTestCase {
	protected static Connection conn;
	protected static Statement stmt;

	@BeforeAll
	static void setUpBeforeClass() throws Exception {
		conn = DBUtils.getConnection(Config.getConfig());
		conn.setAutoCommit(true);
		stmt = conn.createStatement();
	}

	@AfterAll
	static void tearDownAfterClass() throws Exception {
		conn.close();
	}

	protected void truncateTable(String tableName) throws SQLException {
		String sql = "truncate table " + tableName;
		stmt.executeUpdate(sql);
	}

	protected int countRecords(String tableName) throws SQLException {
		String sql = "select count(*) from " + tableName;
		ResultSet rs = stmt.executeQuery(sql);
		if (rs.next()) {
			return rs.getInt(1);
		} else {
			throw new RuntimeException("No records selected.");
		}
	}
}
