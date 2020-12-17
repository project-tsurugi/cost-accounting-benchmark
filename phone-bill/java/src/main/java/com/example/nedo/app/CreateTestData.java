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
		// 引数で、各種パラメータを指定可能にする
		Date start =DBUtils.toDate("2010-11-11");
		Date end = DBUtils.toDate("2020-12-21");
		long numberOfContractsRecords = (long) 1E6;
		TestDataGenerator generator = new TestDataGenerator(0, numberOfContractsRecords, 0, 2, 5, 11, start, end );

		// 契約マスタのテストデータ生成
		long startTime = System.currentTimeMillis();
		generator.generateContract();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "%,d records generated to contracts table in %,.3f sec ";
		System.out.println(String.format(format, numberOfContractsRecords, elapsedTime / 1000d));
	}


}
