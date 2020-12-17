/**
 *
 */
package com.example.nedo.testdata;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.CreateTable;
import com.example.nedo.db.DBUtils;
import com.example.nedo.testdata.ContractsGenerator.Duration;

/**
 * @author umega
 *
 */
class ContractsGeneratorTest extends AbstractDbTestCase {

	/**
	 * generate()のテスト
	 */
	@Test
	void testGenerate() throws SQLException {
		new CreateTable().execute(new String[0]);

		Date start =DBUtils.toDate("2010-11-11");
		Date end = DBUtils.toDate("2020-12-21");
		ContractsGenerator generator = new ContractsGenerator(0, 10000, 2, 5, 11, start, end );
		generator.generate();

		String sql;

		// 100レコード生成されていること
		sql = "select count(*) from contracts";
		assertEquals("10000", execSqlAndGetString(sql));

		// 複数のレコードを持つ電話番号が1000種類存在すること
		sql = "select count(*) from  "
				+ "(select phone_number, count(*) as c from contracts group by phone_number) "
				+ "as dummy where c > 1";
		assertEquals("1000", execSqlAndGetString(sql));

		// end_dateを持たないレコードが6500であること
		sql = "select count(*) from contracts where end_date is null";
		assertEquals("6500", execSqlAndGetString(sql));


	}

	private String execSqlAndGetString(String sql) throws SQLException {
		ResultSet rs = stmt.executeQuery(sql);
		if (rs.next()) {
			return rs.getString(1);
		}
		throw new SQLException("Fail to exece sql:" + sql);
	}

	/**
	 * initDurationList()のテスト
	 */
	@Test
	void testInitDurationList() {
		// 通常ケース
		testInitDurationLisSubt(1, 3, 7, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(13, 5, 2, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		// 一項目が0
		testInitDurationLisSubt(3, 7, 0, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(3, 0, 5, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(0, 7, 5, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		// 二項目が0
		testInitDurationLisSubt(0, 7, 0, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(3, 0, 0, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		testInitDurationLisSubt(0, 0, 5, DBUtils.toDate("2010-11-11"), DBUtils.toDate("2020-01-01"));
		// startの翌日=endのケース
		testInitDurationLisSubt(13, 5, 2, DBUtils.toDate("2020-01-01"), DBUtils.toDate("2020-01-02"));

	}

	void testInitDurationLisSubt(int duplicatePhoneNumberRatio, int expirationDateRate, int noExpirationDateRate
		, Date start, Date end) {
		ContractsGenerator generator = new ContractsGenerator(0, 0, duplicatePhoneNumberRatio, expirationDateRate,
				noExpirationDateRate, start, end);
		List<Duration> list = generator.getDurationList();
		// listの要素数が duplicatePhoneNumberRatio * 2 + expirationDateRate + noExpirationDateRateであること
		assertEquals(duplicatePhoneNumberRatio * 2 + expirationDateRate + noExpirationDateRate, list.size());
		// 始めの、expirationDateRate + noExpirationDateRate 個の要素を調べると、契約終了日が存在する要素数が
		// expirationDateRate, 契約終了日が存在しない要数がnoExpirationDateRateであること。
		int n1 = 0;
		int n2 = 0;
		for(int i = 0; i < expirationDateRate + noExpirationDateRate; i++) {
			Duration d = list.get(i);
			assertNotNull(d.start);
			if (d.end == null) {
				n1++;
			} else {
				n2++;
			}
		}
		assertEquals(noExpirationDateRate, n1);
		assertEquals(expirationDateRate, n2);
		// expirationDateRate + noExpirationDateRateより後の要素は以下の2つの要素のペアが、duplicatePhoneNumberRatio個
		// 続いていること。
		//
		// 1番目の要素の、startがContractsGeneratorのコンストラクタに渡したstartと等しい
		// 2番目の要素のendがnull
		// 2番目の要素のstartが1番目の要素のendより大きい

		for(int i = expirationDateRate + noExpirationDateRate; i < list.size(); i+=2) {
			Duration d1 = list.get(i);
			Duration d2 = list.get(i+1);
			assertEquals(start, d1.start);
			assertTrue(d1.end.getTime() < d2.start.getTime());
			assertNull(d2.end);
		}
	}




	/**
	 * getPhoneNumber()のテスト
	 */
	@Test
	void testGetPhoneNumber() {
		ContractsGenerator generator = new ContractsGenerator(0, 0, 2, 1, 3,
				 DBUtils.toDate("2000-01-01"), DBUtils.toDate("2000-01-01"));
		assertEquals("00000000000", generator.getPhoneNumber(0));
		assertEquals("00000000001", generator.getPhoneNumber(1));
		assertEquals("00000000002", generator.getPhoneNumber(2));
		assertEquals("00000000003", generator.getPhoneNumber(3));

		assertEquals("00000000005", generator.getPhoneNumber(4));
		assertEquals("00000000005", generator.getPhoneNumber(5));
		assertEquals("00000000007", generator.getPhoneNumber(6));
		assertEquals("00000000007", generator.getPhoneNumber(7));

		assertEquals("00000000008", generator.getPhoneNumber(8));
		assertEquals("00000000009", generator.getPhoneNumber(9));
		assertEquals("00000000010", generator.getPhoneNumber(10));
		assertEquals("00000000011", generator.getPhoneNumber(11));

		assertEquals("00000000013", generator.getPhoneNumber(12));
		assertEquals("00000000013", generator.getPhoneNumber(13));
		assertEquals("00000000015", generator.getPhoneNumber(14));
		assertEquals("00000000015", generator.getPhoneNumber(15));

		assertEquals("00000000016", generator.getPhoneNumber(16));
		assertEquals("00000000017", generator.getPhoneNumber(17));
		assertEquals("00000000018", generator.getPhoneNumber(18));

		Exception e;
		e = assertThrows(RuntimeException.class, () -> generator.getPhoneNumber(100000000000L));
		assertEquals("Out of phone number range: 100000000000", e.getMessage());
		e= assertThrows(RuntimeException.class, () -> generator.getPhoneNumber(-1));
		assertEquals("Out of phone number range: -1", e.getMessage());
	}

	/**
	 * getPhoneNumber()のテスト
	 */
	@Test
	void testGetDuration() {
		ContractsGenerator generator = new ContractsGenerator(12, 0, 2, 3, 4,
				DBUtils.toDate("2000-01-01"), DBUtils.toDate("2000-12-01"));
		List<Duration> list = generator.getDurationList();
		for (int i = 0; i < 20; i++) {
			Duration expected = list.get(i % (2 * 2 + 3 + 4));
			Duration actual = generator.getDuration(i);
			assertEquals(expected, actual);
		}

	}

	/**
	 * nextDate()のテスト
	 */
	@Test
	void testNextDate() {
		ContractsGenerator generator = new ContractsGenerator(0, 0, 0, 0,
				0, DBUtils.toDate("2000-01-01"), DBUtils.toDate("2000-01-01"));
		assertEquals(DBUtils.toDate("2020-11-11"), generator.nextDate(DBUtils.toDate("2020-11-10")));
	}

	@Test
	void tesGetDate1() {
		Date start = DBUtils.toDate("2020-11-11");
		Date end = DBUtils.toDate("2020-11-11");
		Set<Date> expected = Collections.singleton(DBUtils.toDate("2020-11-11"));
		Set<Date> actual = new TreeSet<>();

		ContractsGenerator generator = new ContractsGenerator(0, 0, 0, 0, 0, start, end);
		for(int i = 0; i < 100; i++) {
			actual.add(generator.getDate(start, end));
		}
		assertEquals(expected, actual);
	}

	@Test
	void tesGetDate3() {
		Date start = DBUtils.toDate("2020-11-11");
		Date end = DBUtils.toDate("2020-11-13");
		Set<Date> expected = new TreeSet<>(Arrays.asList(
				DBUtils.toDate("2020-11-11"),
				DBUtils.toDate("2020-11-12"),
				DBUtils.toDate("2020-11-13")));
		Set<Date> actual = new TreeSet<>();

		ContractsGenerator generator = new ContractsGenerator(0, 0, 0, 0, 0, start, end);
		for(int i = 0; i < 100; i++) {
			actual.add(generator.getDate(start, end));
		}
		assertEquals(expected, actual);
	}

	@Test
	void tesGetDate7() {
		Date start = DBUtils.toDate("2020-11-11");
		Date end = DBUtils.toDate("2020-11-17");
		Set<Date> expected = new TreeSet<>(Arrays.asList(
				DBUtils.toDate("2020-11-11"),
				DBUtils.toDate("2020-11-12"),
				DBUtils.toDate("2020-11-13"),
				DBUtils.toDate("2020-11-14"),
				DBUtils.toDate("2020-11-15"),
				DBUtils.toDate("2020-11-16"),
				DBUtils.toDate("2020-11-17")));
		Set<Date> actual = new TreeSet<>();

		ContractsGenerator generator = new ContractsGenerator(0, 0, 0, 0, 0, start, end);
		for(int i = 0; i < 100; i++) {
			actual.add(generator.getDate(start, end));
		}
		assertEquals(expected, actual);
	}




}
