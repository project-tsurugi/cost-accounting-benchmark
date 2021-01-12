package com.example.nedo;

import java.sql.Connection;
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
}
