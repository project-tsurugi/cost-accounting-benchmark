package com.example.nedo.testdata;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.db.DBUtils;

public class CreateTestData implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(CreateTestData.class);

    public static void main(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		CreateTestData createTestData = new CreateTestData();
		createTestData.execute(config);
	}


	@Override
	public void execute(Config config) throws Exception {
		TestDataGenerator generator = new TestDataGenerator(config);

		// 契約マスタのテストデータ生成
		long startTime = System.currentTimeMillis();
		generator.generateContract();
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "%,d records generated to contracts table in %,.3f sec ";
		LOG.info(String.format(format, config.numberOfContractsRecords, elapsedTime / 1000d));

		// 通話履歴のテストデータを作成
		startTime = System.currentTimeMillis();
		generator.generateHistory(DBUtils.toDate("2020-11-01"), DBUtils.toDate("2021-01-10"));
		elapsedTime = System.currentTimeMillis() - startTime;
		format = "%,d records generated to history table in %,.3f sec ";
		LOG.info(String.format(format, config.numberOfHistoryRecords, elapsedTime / 1000d));
	}
}
