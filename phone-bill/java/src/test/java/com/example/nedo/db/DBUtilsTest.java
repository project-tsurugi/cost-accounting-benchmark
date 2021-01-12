package com.example.nedo.db;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

import com.example.nedo.app.Config;

class DBUtilsTest {
	/**
	 * getConnection()のテスト
	 * @throws SQLException
	 * @throws IOException
	 */
	@Test
	void testGetConnection() throws SQLException, IOException {
		Config config = Config.getConfig(new String[0]);
		Connection conn = DBUtils.getConnection(config);
		assertTrue(conn.isValid(1));
	}

	/**
	 * toDate()のテスト
	 */
	@Test
	void testToDate() {
		assertEquals("2010-01-13", DBUtils.toDate("2010-01-13").toString());
	}

	/**
	 * toTimestamp()のテスト
	 */
	@Test
	void testToTimestamp() {
		assertEquals("2010-01-13 18:12:21.999", DBUtils.toTimestamp("2010-01-13 18:12:21.999").toString());
		assertEquals("2010-01-13 18:12:21.0", DBUtils.toTimestamp("2010-01-13 18:12:21.000").toString());
	}

	/**
	 * nextDate()のテスト
	 */
	@Test
	void testNextDate() {
		assertEquals(DBUtils.toDate("2020-11-11"), DBUtils.nextDate(DBUtils.toDate("2020-11-10")));
	}
}
