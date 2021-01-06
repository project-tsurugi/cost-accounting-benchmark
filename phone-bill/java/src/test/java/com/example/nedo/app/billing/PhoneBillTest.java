package com.example.nedo.app.billing;

import static org.junit.jupiter.api.Assertions.*;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.example.nedo.app.CreateTable;
import com.example.nedo.db.Billing;
import com.example.nedo.db.DBUtils;

class PhoneBillTest {
	Connection conn;

	@Test
	void test() throws SQLException {
		// 初期化
		conn = DBUtils.getConnection();
		conn.setAutoCommit(true);
		CreateTable.main(new String[0]);
		PhoneBill phoneBill = new PhoneBill();

		// データが存在しない状態での料金計算
		phoneBill.doCalc(DBUtils.toDate("2020-11-01"), DBUtils.toDate("2020-11-30"));
		assertEquals(0, getBillings().size());

		// 契約マスタにテストデータをセット
		insertToContracts("Phone-0001", "2010-01-01", null, "Simple"); 			// 有効な契約
		insertToContracts("Phone-0002", "2010-01-01", "2020-10-31", "Simple"); 	// 終了した契約(境界値)
		insertToContracts("Phone-0003", "2010-01-01", "2020-11-01", "Simple");	// 有効な契約2(境界値)
		insertToContracts("Phone-0004", "2020-11-30", null, "Simple"); 			// 有効な契約3(境界値)
		insertToContracts("Phone-0005", "2020-11-30", "2021-01-10", "Simple"); 	// 有効な契約4(境界値)
		insertToContracts("Phone-0006", "2020-12-01", "2021-01-10", "Simple"); 	// 未来の契約(境界値)
		insertToContracts("Phone-0007", "2020-12-01", null, "Simple"); 			// 未来の契約(境界値)
		insertToContracts("Phone-0008", "2010-01-01", "2018-11-10", "Simple"); 	// 同一電話番号の複数の契約
		insertToContracts("Phone-0008", "2020-01-21", null, "Simple"); 			// 同一電話番号の複数の契約


		// 通話履歴がない状態での料金計算
		phoneBill.doCalc(DBUtils.toDate("2020-11-01"), DBUtils.toDate("2020-11-30"));
		List<Billing> list = getBillings();
		assertEquals(5, list.size());
		assertEquals(toBilling("Phone-0001", "2020-11-01", 3000, 0, 3000), list.get(0));
		assertEquals(toBilling("Phone-0003", "2020-11-01", 3000, 0, 3000), list.get(1));
		assertEquals(toBilling("Phone-0004", "2020-11-01", 3000, 0, 3000), list.get(2));
		assertEquals(toBilling("Phone-0005", "2020-11-01", 3000, 0, 3000), list.get(3));
		assertEquals(toBilling("Phone-0008", "2020-11-01", 3000, 0, 3000), list.get(4));

		// 通話履歴ありの場合
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-10-31 23:59:59.999", 30, false);		// 計算対象年月外
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-01 00:00:00.000", 30, false);  	// 計算対象
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-15 12:12:12.000", 90, true); 	 	// 削除フラグ
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-11-30 23:59:59.999", 90, false);  	// 計算対象
		insertToHistory("Phone-0001", "Phone-0008", "C", "2020-12-01 00:00:00.000", 30, false);  	// 計算対象年月外
		insertToHistory("Phone-0005", "Phone-0001", "C", "2020-11-10 00:00:00.000", 30, false);  	// 計算対象
		insertToHistory("Phone-0005", "Phone-0008", "R", "2020-11-30 00:00:00.000", 30, false);  	// 計算対象(受信者負担)



	}


	private List<Billing> getBillings() throws SQLException {
		List<Billing> list = new ArrayList<Billing>();
		String sql = "select phone_number, target_month, basic_charge, metered_charge, billing_amount"
				+ " from billing order by phone_number, target_month";
		try (Connection conn = DBUtils.getConnection()) {
			Statement stmt = conn.createStatement();
			ResultSet rs = stmt.executeQuery(sql);
			while (rs.next()) {
				Billing billing = new Billing();
				billing.phoneNumber = rs.getString(1);
				billing.targetMonth = rs.getDate(2);
				billing.basicCharge = rs.getInt(3);
				billing.meteredCharge = rs.getInt(4);
				billing.billingAmount = rs.getInt(5);
				list.add(billing);
			}
		}
		return list;
	}

	private Billing toBilling(String phoneNumber, String targetMonth, int basicCharge, int meteredCharge,
			int billingAmount) {
		Billing billing = new Billing();
		billing.phoneNumber = phoneNumber;
		billing.targetMonth = DBUtils.toDate(targetMonth);
		billing.basicCharge = basicCharge;
		billing.meteredCharge = meteredCharge;
		billing.billingAmount = billingAmount;
		return billing;
	}


	/**
	 * 契約マスタにレコードを追加する
	 *
	 * @param phoneNumber
	 * @param startDate
	 * @param endDate
	 * @param chargeRule
	 * @throws SQLException
	 */
	private void insertToContracts(String phoneNumber, String startDate, String endDate, String chargeRule)
			throws SQLException {
		String sql = "insert into contracts(phone_number, start_date, end_date, charge_rule) values(?, ?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, phoneNumber);
			ps.setDate(2, DBUtils.toDate(startDate));
			if (endDate == null) {
				ps.setNull(3, Types.DATE);
			} else {
				ps.setDate(3, DBUtils.toDate(endDate));
			}
			ps.setString(4, chargeRule);
			int c = ps.executeUpdate();
			assertEquals(1, c);
		}
	}

	/**
	 * 履歴テーブルにレコードを追加する
	 *
	 * @param caller_phone_number 発信者電話番号
	 * @param recipient_phone_number 受信者電話番号
	 * @param payment_categorty	料金区分
	 * @param start_time 通話開始時刻
	 * @param time_secs 通話時間
	 * @param df 論理削除フラグ
	 * @throws SQLException
	 */
	private void insertToHistory(String caller_phone_number, String recipient_phone_number, String payment_categorty, String start_time, int time_secs, boolean df)
			throws SQLException {
		String sql = "insert into history(caller_phone_number, recipient_phone_number, payment_categorty, start_time, time_secs, charge, df) values(?, ?, ?, ?, ?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, caller_phone_number);
			ps.setString(2, recipient_phone_number);
			ps.setString(3, payment_categorty);
			ps.setTimestamp(4, DBUtils.toTimestamp(start_time));
			ps.setInt(5, time_secs);
			ps.setNull(6, Types.INTEGER);
			ps.setBoolean(7, df);
			int c = ps.executeUpdate();
			assertEquals(1, c);
		}
	}
}



