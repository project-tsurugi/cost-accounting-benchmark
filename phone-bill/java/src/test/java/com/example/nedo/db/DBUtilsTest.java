package com.example.nedo.db;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.SQLException;

import org.junit.jupiter.api.Test;

class DBUtilsTest {
	@Test
	void testGetConnection() throws SQLException {
		Connection conn = DBUtils.getConnection();
		assertTrue(conn.isValid(1));
	}

	@Test
	void testToDate() {
		assertEquals("2010-01-13", DBUtils.toDate("2010-01-13").toString());
	}

}
