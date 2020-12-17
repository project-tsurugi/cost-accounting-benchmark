package com.example.nedo.app;

import java.sql.Date;
import java.sql.SQLException;

import com.example.nedo.db.DBUtils;
import com.example.nedo.testdata.ContractsGenerator;

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
		long numberOfRecords = (long) 1E6;
		ContractsGenerator generator = new ContractsGenerator(0, numberOfRecords, 2, 5, 11, start, end );
		long startTime = System.currentTimeMillis();
		generator.generate();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "%,d records generated to contracts table in %,.3f sec ";
		System.out.println(String.format(format, numberOfRecords, elapsedTime / 1000d));
	}


}
