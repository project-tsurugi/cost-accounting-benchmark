package com.example.nedo.online;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.List;
import java.util.Random;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.testdata.TestDataGenerator;

/**
 * 指定の頻度で、契約マスタにレコードをインサートするアプリケーション
 *
 */
public class MasterInsertApp extends AbstractOnlineApp {
	private TestDataGenerator testDataGenerator;
	private ContractKeyHolder contractKeyHolder;

	public MasterInsertApp(ContractKeyHolder contractKeyHolder, Config config, Random random) throws SQLException {
		super(config.masterInsertReccrdsPerMin, config, random);
		testDataGenerator = new TestDataGenerator(config);
		this.contractKeyHolder = contractKeyHolder;
	}

	@Override
	void exec() throws SQLException {
		Connection conn = getConnection();
		PreparedStatement ps = conn.prepareStatement(TestDataGenerator.SQL_INSERT_TO_CONTRACT);
		int n = contractKeyHolder.size();
		Contract c = testDataGenerator.setContract(ps, n);
		ps.executeUpdate();
		contractKeyHolder.add(ContractKeyHolder.createKey(c.phoneNumber, c.startDate));
	}


	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) {
		// Nothing to do
	}

}
