package com.example.nedo.online;

import java.sql.Date;
import java.sql.SQLException;
import java.util.Random;

import com.example.nedo.app.Config;
import com.example.nedo.db.DBUtils;
import com.example.nedo.testdata.TestDataGenerator;

public class HistoryInsertApp extends AbstractOnlineApp {
	private TestDataGenerator testDataGenerator;
	private int historyInsertRecordsPerTransaction;

	public HistoryInsertApp(Config config, Random random) throws SQLException {
		super(config.historyInsertTransactionPerMin, config, random);
		historyInsertRecordsPerTransaction = config.historyInsertRecordsPerTransaction;
		testDataGenerator = new TestDataGenerator(config, random);
	}

	@Override
	void exec() throws SQLException {
		Date date = new Date(System.currentTimeMillis()); // TODO 実行時の日付でなく設定ファイルで指定する
		testDataGenerator.generateHistory(date, DBUtils.nextDate(date), historyInsertRecordsPerTransaction);
	}

}
