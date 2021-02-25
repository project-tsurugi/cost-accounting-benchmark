package com.example.nedo.online;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import com.example.nedo.app.Config;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;
import com.example.nedo.testdata.TestDataGenerator;

/**
 * 指定の頻度で、契約マスタにレコードをインサートするアプリケーション
 *
 */
public class MasterInsertApp extends AbstractOnlineApp {
	private Connection conn;
	private TestDataGenerator testDataGenerator;
	ContractKeyHolder contractKeyHolder;

	public MasterInsertApp(ContractKeyHolder contractKeyHolder, Config config, Random random) throws SQLException {
		super(config.masterInsertReccrdsPerMin, random);
		conn = DBUtils.getConnection(config);
		conn.setAutoCommit(true);
		testDataGenerator = new TestDataGenerator(config);
		this.contractKeyHolder = contractKeyHolder;
	}

	@Override
	void exec() throws SQLException {
		PreparedStatement ps = conn.prepareStatement("insert into contracts("
				+ "phone_number,"
				+ "start_date,"
				+ "end_date,"
				+ "charge_rule"
				+ ") values(?, ?, ?, ?)");
		int n = contractKeyHolder.size();
		String phoneNumber = testDataGenerator.getPhoneNumber(n);
		Duration d = testDataGenerator.getDuration(n);
		String rule = "Simple";
		ps.setString(1, phoneNumber);
		ps.setDate(2, d.start);
		ps.setDate(3, d.end);
		ps.setString(4, rule);
		ps.executeUpdate();
		contractKeyHolder.add(ContractKeyHolder.createKey(phoneNumber, d.start));
	}


	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) {
		// Nothing to do
	}

	@Override
	protected void cleanup() throws SQLException {
		if (conn != null & !conn.isClosed()) {
			conn.close();
		}
	}

}
