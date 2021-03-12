package com.example.nedo.online;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Random;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.db.Contract;
import com.example.nedo.online.ContractKeyHolder.Key;

public class MasterUpdateApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(MasterUpdateApp.class);

	private static final long DAY_IN_MILLS = 24 * 3600 * 1000;

	private ContractKeyHolder contractKeyHolder;
	private Config config;
	private Random random;
	private Updater[] updaters = {new Updater1(), new Updater2()};


	public MasterUpdateApp(ContractKeyHolder contractKeyHolder, Config config, Random random) throws SQLException {
		super(config.masterUpdateRecordsPerMin, config, random);
		this.config = config;
		this.random = random;
		this.contractKeyHolder = contractKeyHolder;
	}



	@Override
	void exec() throws SQLException {
		// 更新対象レコードを選択
		int n = random.nextInt(contractKeyHolder.size());
		Key key = contractKeyHolder.get(n);
		Contract contract = getContract(key);
		// 更新対象レコードを更新してDBに反映する
		Updater updater = updaters[random.nextInt(updaters.length)];
		updater.update(contract);
		updateDatabase(contract);
		LOG.info("ONLINE APP: Update 1 record from contracs.");
	}

	/**
	 * 指定のKEYの契約を取得する
	 *
	 * @param key
	 * @return
	 * @throws SQLException
	 */
	Contract getContract(Key key) throws SQLException {
		Connection conn = getConnection();
		String sql = "select end_date, charge_rule from contracts where phone_number = ? and start_date = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, key.phoneNumber);
			ps.setDate(2, key.startDate);
			ResultSet rs = ps.executeQuery();
			if (rs.next()) {
				Contract c = new Contract();
				c.phoneNumber = key.phoneNumber;
				c.startDate = key.startDate;
				c.endDate = rs.getDate(1);
				c.rule = rs.getString(2);
				return c;
			} else {
				throw new RuntimeException("No records selected.");
			}
		}
	}

	/**
	 * 契約を更新する
	 *
	 * @param key
	 * @return
	 * @throws SQLException
	 */
	void updateDatabase(Contract contract) throws SQLException {
		Connection conn = getConnection();
		String sql = "update contracts set end_date = ?, charge_rule = ? where phone_number = ? and start_date = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, contract.endDate);
			ps.setString(2, contract.rule);
			ps.setString(3, contract.phoneNumber);
			ps.setDate(4, contract.startDate);
			int ret = ps.executeUpdate();
			if (ret != 1) {
				throw new RuntimeException("Fail to update contracts: " + contract);
			}
		}
	}

	// 契約を更新するInterfaceと、Interfaceを実装したクラス

	interface Updater {
		/**
		 * Contactの値を更新する
		 *
		 * @param contract
		 */
		void update(Contract contract);
	}

	/**
	 * 契約終了日を削除する
	 *
	 */
	class Updater1 implements Updater {
		@Override
		public void update(Contract contract) {
			contract.endDate = null;
		}
	}

	/**
	 * 契約終了日を設定する
	 *
	 */
	class Updater2 implements Updater {
		@Override
		public void update(Contract contract) {
			long startTime = contract.startDate.getTime();
			int d = (int) ((config.maxDate.getTime() - startTime) / DAY_IN_MILLS);
			int r = random.nextInt(d + 1);
			contract.endDate = new Date(startTime + r * DAY_IN_MILLS);
		}
	}

}
