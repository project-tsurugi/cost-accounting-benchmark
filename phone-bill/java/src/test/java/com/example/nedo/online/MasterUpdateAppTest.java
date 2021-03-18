package com.example.nedo.online;

import static org.junit.jupiter.api.Assertions.*;

import java.io.IOException;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.junit.jupiter.api.Test;

import com.example.nedo.AbstractDbTestCase;
import com.example.nedo.app.Config;
import com.example.nedo.app.CreateTable;
import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;
import com.example.nedo.online.MasterUpdateApp.Updater;
import com.example.nedo.testdata.TestDataGenerator;

class MasterUpdateAppTest extends AbstractDbTestCase {

	@Test
	void testExec() throws IOException, Exception {
		// テーブルにテストデータを入れる
		Config config = Config.getConfig();
		config.numberOfContractsRecords = 30;
		new CreateTable().execute(config);
		TestDataGenerator generator = new TestDataGenerator(config);
		generator.generateContracts();

		ContractKeyHolder keyHolder = new ContractKeyHolder(config);
		MasterUpdateApp app = new MasterUpdateApp(keyHolder, config, new Random(0));
		List<Contract> expected = getContracts();


		app.getConnection().commit();
		expected.get(0).endDate = null;
		testContracts(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(17).endDate = null;
		testContracts(expected);

		app.exec();
		app.getConnection().commit();
		testContracts(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(11).endDate = DBUtils.toDate("2021-02-08");;
		testContracts(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(17).endDate = null;
		testContracts(expected);

		app.exec();
		app.getConnection().commit();
		expected.get(22).endDate = DBUtils.toDate("2021-01-29");
		testContracts(expected);

	}



	private void testContracts(List<Contract> expected) throws SQLException {
		List<Contract> actual = getContracts();
		assertEquals(expected.size(), actual.size());
		for (int i = 0; i < expected.size(); i++) {
			assertEquals(expected.get(i), actual.get(i));
		}
	}



	private List<Contract> getContracts() throws SQLException {
		List<Contract> contracts = new ArrayList<Contract>();
		String sql = "select phone_number, start_date, end_date, charge_rule"
				+ " from contracts order by phone_number, start_date";
		ResultSet rs = stmt.executeQuery(sql);
		while (rs.next()) {
			Contract c = new Contract();
			c.phoneNumber = rs.getString(1);
			c.startDate = rs.getDate(2);
			c.endDate = rs.getDate(3);
			c.rule = rs.getString(4);
			contracts.add(c);
		}
		return contracts;
	}


//	@Test
//	void testGetContractAndUpdateDatabase() throws IOException, Exception {
//		// テーブルにテストデータを入れる
//
//		Config config = Config.getConfig();
//		config.minDate = DBUtils.toDate("2010-01-11");
//		config.maxDate = DBUtils.toDate("2020-12-21");
//		config.numberOfContractsRecords = 100;
//		config.expirationDateRate =5;
//		config.noExpirationDateRate = 11;
//		config.duplicatePhoneNumberRatio = 2;
//		new CreateTable().execute(config);
//		TestDataGenerator generator = new TestDataGenerator(config);
//		generator.generateContracts();
//
//		ContractKeyHolder keyHolder = new ContractKeyHolder(config);
//		MasterUpdateApp app = new MasterUpdateApp(keyHolder, config, null);
//
//		// getContractのテスト
//		Contract contract11 = app.getContract(keyHolder.get(11));
//		app.getConnection().commit();
//		assertEquals("00000000011", contract11.phoneNumber);
//		assertEquals(DBUtils.toDate("2010-08-28"), contract11.startDate);
//		assertEquals(DBUtils.toDate("2014-02-22"), contract11.endDate);
//		assertEquals("sample", contract11.rule);
//
//		Contract contract42 = app.getContract(keyHolder.get(42));
//		app.getConnection().commit();
//		assertEquals("00000000042", contract42.phoneNumber);
//		assertEquals(DBUtils.toDate("2016-09-07"), contract42.startDate);
//		assertNull(contract42.endDate);
//		assertEquals("sample", contract42.rule);
//
//		// マッチするレコードがないキーを指定したとき
//		Key key = keyHolder.get(10);
//		key.startDate = DBUtils.nextDate(key.startDate);
//		RuntimeException e1 = assertThrows(RuntimeException.class, () -> app.getContract(key));
//		app.getConnection().commit();
//		assertEquals("No records selected.", e1.getMessage());
//
//		// updateDatabaseのテスト
//		contract42.endDate = DBUtils.toDate("2016-05-18");
//		contract42.rule = "another rule";
//		app.updateDatabase(contract42);
//		app.getConnection().commit();
//		assertEquals(contract42, app.getContract(keyHolder.get(42)));
//
//		// キーが一致するレコードが存在しない場合
//		contract42.phoneNumber = "NotExists";
//		contract42.endDate = null;
//		contract42.rule = "sample";
//		RuntimeException e2 = assertThrows(RuntimeException.class, () -> app.updateDatabase(contract42));
//		app.getConnection().commit();
//		assertEquals(
//				"Fail to update contracts: "
//				+ "Contract [phone_number=NotExists, start_date=2016-09-07, end_date=null, rule=sample]",
//				e2.getMessage());
//	}


	@Test
	void testCommonDuration() {
		Contract c1 = new Contract();
		Contract c2 = new Contract();
		List<Contract> contracts = Arrays.asList(c2);

		///////////////////////////////////////////////////// contractsの要素数が1のとき
		c1.startDate = DBUtils.toDate("2018-01-01");
		c1.endDate = DBUtils.toDate("2018-05-01");

		// C1の期間にC2の期間が含まれるケース
		c2.startDate = DBUtils.toDate("2018-03-10");
		c2.endDate = DBUtils.toDate("2018-04-01");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));


		// C2の期間にC1の期間が含まれるケース
		c2.startDate = DBUtils.toDate("2017-01-10");
		c2.endDate = DBUtils.toDate("2019-05-01");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));


		// c1 < c2 のケース
		c2.startDate = DBUtils.toDate("2019-01-10");
		c2.endDate = DBUtils.toDate("2019-05-01");
		assertFalse(MasterUpdateApp.commonDuration(c1, contracts));

		// c1の終了月とc2の開始月が連続する月になるケース(c1の開始日の方がc2の開始日より早い)
		c2.startDate = DBUtils.toDate("2018-06-01");
		c2.endDate = DBUtils.toDate("2019-05-01");
		assertFalse(MasterUpdateApp.commonDuration(c1, contracts));


		// c1の終了月とc2の開始月が一致するケース
		c2.startDate = DBUtils.toDate("2018-05-01");
		c2.endDate = DBUtils.toDate("2019-05-01");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

		c2.startDate = DBUtils.toDate("2018-05-31");
		c2.endDate = DBUtils.toDate("2019-05-01");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

		// c1の一部の期間とc2の一部の期間がかぶるとき(c1の開始日の方がc2の開始日より早い)
		c2.startDate = DBUtils.toDate("2018-03-01");
		c2.endDate = DBUtils.toDate("2019-05-01");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

		// c1の一部の期間とc2の一部の期間がかぶるとき(c1の開始日の方がc2の開始日より遅い)
		c2.startDate = DBUtils.toDate("2017-06-01");
		c2.endDate = DBUtils.toDate("2018-02-31");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

		// c1の開始月とc2の終了月が一致するケース
		c2.startDate = DBUtils.toDate("2017-06-01");
		c2.endDate = DBUtils.toDate("2018-01-31");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

		c2.startDate = DBUtils.toDate("2017-06-01");
		c2.endDate = DBUtils.toDate("2018-01-01");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));

		// c1の終了月とc2の開始月が連続する月になるケース(c1の開始日の方がc2の開始日より遅い)
		c2.startDate = DBUtils.toDate("2017-06-01");
		c2.endDate = DBUtils.toDate("2017-12-31");
		assertFalse(MasterUpdateApp.commonDuration(c1, contracts));

		// c2 < c1 のケース
		c2.startDate = DBUtils.toDate("2015-01-10");
		c2.endDate = DBUtils.toDate("2015-05-01");
		assertFalse(MasterUpdateApp.commonDuration(c1, contracts));

		///////////////////////////////////////////////////// contractsに複数の要素があるケース

		Contract c3 = new Contract();
		Contract c4 = new Contract();
		contracts = Arrays.asList(c2, c3, c4);


		// 重複期間が一つもないケース
		c1.startDate = DBUtils.toDate("2018-01-01");
		c1.endDate = DBUtils.toDate("2018-05-01");
		c2.startDate = DBUtils.toDate("2019-03-10");
		c2.endDate = DBUtils.toDate("2019-04-01");
		c3.startDate = DBUtils.toDate("2020-03-10");
		c3.endDate = DBUtils.toDate("2020-04-01");
		c4.startDate = DBUtils.toDate("2021-03-10");
		c4.endDate = DBUtils.toDate("2021-04-01");
		assertFalse(MasterUpdateApp.commonDuration(c1, contracts));


		// 期間が重複する契約が一つだけあるケース
		c1.startDate = DBUtils.toDate("2019-01-01");
		c1.endDate = DBUtils.toDate("2019-12-01");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));


		// すべての契約の期間が重複するケース
		c1.startDate = DBUtils.toDate("2000-01-01");
		c1.endDate = DBUtils.toDate("2119-12-01");
		assertTrue(MasterUpdateApp.commonDuration(c1, contracts));
	}


	@Test
	void testUpdater1() throws SQLException, IOException {
		MasterUpdateApp app = new MasterUpdateApp(null, Config.getConfig(), new Random());
		Updater updater = app.new Updater1();

		Contract contract = new Contract();
		contract.endDate = DBUtils.toDate("2020-02-22");
		updater.update(contract);
		assertNull(contract.endDate);
	}

	@Test
	void testUpdater2() throws IOException, SQLException {
		Config config = Config.getConfig();
		config.minDate = DBUtils.toDate("2010-12-15");
		config.maxDate = DBUtils.toDate("2020-02-15");
		MasterUpdateApp app = new MasterUpdateApp(null, config, new Random());
		Updater updater = app.new Updater2();

		// ContractのstartDateと、ConfigのmaxDateが等しい場合、Contract.endDateも同じ値になる
		for (int i = 0; i < 100; i++) {
			Contract contract = new Contract();
			contract.startDate = config.maxDate;
			contract.endDate = null;
			updater.update(contract);
			assertEquals(DBUtils.toDate("2020-02-15"), contract.endDate);
		}

		// Contract.endDateが、Contract.startDate と Config.maxDateの間の値になることを確認
		Set<Date> actual = new HashSet<Date>();
		Set<Date> expected = new HashSet<Date>();
		expected.add(DBUtils.toDate("2020-02-13"));
		expected.add(DBUtils.toDate("2020-02-14"));
		expected.add(DBUtils.toDate("2020-02-15"));

		for (int i = 0; i < 100; i++) {
			Contract contract = new Contract();
			contract.startDate = DBUtils.toDate("2020-02-13");
			contract.endDate = null;
			updater.update(contract);
			actual.add(contract.endDate);
		}
		assertEquals(expected, actual);
	}
}
