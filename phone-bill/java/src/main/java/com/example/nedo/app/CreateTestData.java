package com.example.nedo.app;

import java.sql.Date;
import java.sql.SQLException;

import com.example.nedo.db.DBUtils;
import com.example.nedo.testdata.TestDataGenerator;

public class CreateTestData implements ExecutableCommand {
	public static void main(String[] args) throws SQLException {
		CreateTestData createTestData = new CreateTestData();
		createTestData.execute(args);
	}


	@Override
	public void execute(String[] args) throws SQLException {
		// TODO 引数で、各種パラメータを指定可能にする
		Date start =DBUtils.toDate("2010-11-11");
		Date end = DBUtils.toDate("2020-12-21");
		long numberOfContractsRecords = (long) 1E3;
		long numberOfhistoryRecords = (long) 1E4;
		TestDataGenerator generator = new TestDataGenerator(0, numberOfContractsRecords, numberOfhistoryRecords, 200,
				500, 1100, start, end);

		// 契約マスタのテストデータ生成
		long startTime = System.currentTimeMillis();
		generator.generateContract();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "%,d records generated to contracts table in %,.3f sec ";
		System.out.println(String.format(format, numberOfContractsRecords, elapsedTime / 1000d));

		// 通話履歴のテストデータを作成
		startTime = System.currentTimeMillis();
		generator.generateHistory(DBUtils.toDate("2020-01-01"), DBUtils.toDate("2020-11-30"));
		elapsedTime = System.currentTimeMillis() - startTime;
		format = "%,d records generated to history table in %,.3f sec ";
		System.out.println(String.format(format, numberOfhistoryRecords, elapsedTime / 1000d));
	}

}
