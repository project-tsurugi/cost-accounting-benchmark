package com.example.nedo.online;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Random;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.History;
import com.example.nedo.testdata.TestDataGenerator;

public class HistoryInsertApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(HistoryInsertApp.class);
	private TestDataGenerator testDataGenerator;
	private int historyInsertRecordsPerTransaction;
	private long baseTime;
	private Random random;
	private Connection conn;
	/**
	 * // 同一の時刻のレコードを生成しないために時刻を記録するためのセット
	 */
	private Set<Long> startTimeSet;

	public HistoryInsertApp(Config config, Random random) throws SQLException {
		super(config.historyInsertTransactionPerMin, config, random);
		this.random = random;
		historyInsertRecordsPerTransaction = config.historyInsertRecordsPerTransaction;
		testDataGenerator = new TestDataGenerator(config, random);
		baseTime = getMaxStartTime(); // 通話履歴中の最新の通話開始時刻をベースに新規に作成する通話履歴の通話開始時刻を生成する
		conn = getConnection();
		startTimeSet = new HashSet<Long>(); // 同一の時刻のレコードを生成しないために時刻を記録するためのセット
	}

	@Override
	void exec() throws SQLException {
		List<History> histories = new ArrayList<>();
		long startTime;
		for (int i = 0; i < historyInsertRecordsPerTransaction; i++) {
			do {
				startTime = baseTime + random.nextInt(CREATE_SCHEDULE_INTERVAL_MILLS);
			} while (startTimeSet.contains(startTime));
			startTimeSet.add(startTime);
			histories.add(testDataGenerator.createHistoryRecord(startTime));
		}
		testDataGenerator.insrtHistories(conn, histories);
		conn.commit();
		LOG.info("ONLINE APP: Insert {} records to history.", historyInsertRecordsPerTransaction);
	}

	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) {
		baseTime += CREATE_SCHEDULE_INTERVAL_MILLS;
		startTimeSet.clear();
	}

	long getMaxStartTime() throws SQLException {
		Connection conn = getConnection();
		Statement stmt = conn.createStatement();
		ResultSet rs = stmt.executeQuery("select max(start_time) from history");
		if (rs.next()) {
			Timestamp ts = rs.getTimestamp(1);
			return ts == null ? System.currentTimeMillis() : ts.getTime();
		}
		conn.commit();
		throw new IllegalStateException();
	}
}
