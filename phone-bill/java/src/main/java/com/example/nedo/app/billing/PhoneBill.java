package com.example.nedo.app.billing;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;

public class PhoneBill implements ExecutableCommand {
	public static void main(String[] args) throws SQLException {
		PhoneBill phoneBill = new PhoneBill();
		phoneBill.execute(args);
	}

	@Override
	public void execute(String[] args) throws SQLException {
		// TODO 引数パラメータの処理を追加
		Date start = DBUtils.toDate("2020-12-01");
		Date end = DBUtils.toDate("2021-01-01");
		doCalc(start, end);

	}

	void doCalc(Date start, Date end) throws SQLException {
		long startTime = System.currentTimeMillis();
		try (Connection conn = DBUtils.getConnection()) {
			try {
				deleteTargetManthRecords(conn, start);
				try (ResultSet contractResultSet = getContractResultSet(conn, start, end)) {
					while (contractResultSet.next()) {
						Contract contract = getContract(contractResultSet);
						System.out.println(contract);
						// TODO 契約内容に合致した、CallChargeCalculator, BillingCalculatorを生成するようにする。
						CallChargeCalculator callChargeCalculator = new SimpleCallChargeCalculator();
						BillingCalculator billingCalculator = new SimpleBillingCalculator();
						try (ResultSet historyResultSet = getHistoryResultSet(conn, contract, start, end)) {
							while (historyResultSet.next()) {
								int time = historyResultSet.getInt("time_secs"); // 通話時間を取得
								if (time < 0) {
									throw new RuntimeException("Negative time: " + time);
								}
								int callCharge = callChargeCalculator.calc(time);
								historyResultSet.updateInt("charge", callCharge);
								historyResultSet.updateRow();
								billingCalculator.addCallCharge(callCharge);
							}
						}
						updateBilling(conn, contract, billingCalculator, start);
					}
				}
				conn.commit();
			} catch (Exception e) {
				conn.rollback();
				throw e;
			}
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Billings calculated in %,.3f sec ";
		System.out.println(String.format(format, elapsedTime / 1000d));

	}

	private void updateBilling(Connection conn, Contract contract, BillingCalculator billingCalculator,
			Date targetMonth) throws SQLException {
		String sql = "insert into billing(phone_number, target_month, basic_charge, metered_charge, billing_amount)"
				+ " values(?, ?, ?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, contract.phoneNumber);
			ps.setDate(2, targetMonth);
			ps.setInt(3, billingCalculator.getBasicCharge());
			ps.setInt(4, billingCalculator.getMeteredCharge());
			ps.setInt(5, billingCalculator.getBillingAmount());
			ps.executeUpdate();
		}
	}

	private void deleteTargetManthRecords(Connection conn, Date start) throws SQLException {
		String sql = "delete from billing where target_month = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, start);
			ps.executeUpdate();
		}
	}

	private Contract getContract(ResultSet rs) throws SQLException {
		Contract contract = new Contract();
		contract.phoneNumber = rs.getString(1);
		contract.startDate = rs.getDate(2);
		contract.endDate = rs.getDate(3);
		contract.rule = rs.getString(4);
		return contract;
	}



	/**
	 * historyテーブルから指定のcontractの料金計算対象のレコードを操作するためのResultSetを取り出す
	 *
	 * @param contract 検索対象の契約
	 * @param start 検索対象期間の開始日
	 * @param end 検索対象期間の最終日
	 * @return
	 * @throws SQLException
	 */
	private ResultSet getHistoryResultSet(Connection conn, Contract contract, Date start, Date end)
			throws SQLException {
		String sql = "select caller_phone_number, start_time, time_secs, charge"
				+ " from history "
				+ "where start_time >= ? and start_time < ?"
				+ " and ((caller_phone_number = ? and payment_categorty = 'C') "
				+ "  or (recipient_phone_number = ? and payment_categorty = 'R'))"
				+ " and df = false";

		PreparedStatement ps = conn.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY	, ResultSet.CONCUR_UPDATABLE);
		ps.setDate(1, start);
		ps.setDate(2, DBUtils.nextDate(end));
		ps.setString(3, contract.phoneNumber);
		ps.setString(4, contract.phoneNumber);
		ResultSet rs = ps.executeQuery();
		return rs;
	}

	/**
	 * 契約期間がstart～endと被るcontractテーブルのレコードのResultSetを取得する
	 *
	 * @param start 検索対象期間の開始日
	 * @param end 検索対象期間の最終日
	 * @return
	 * @throws SQLException
	 */
	private ResultSet getContractResultSet(Connection conn, Date start, Date end) throws SQLException {
		String sql = "select phone_number, start_date, end_date, charge_rule"
				+ " from contracts where start_date <= ? and ( end_date is null or end_date >= ?)"
				+ " order by phone_number";
		PreparedStatement ps = conn.prepareStatement(sql);
		ps.setDate(1, end);
		ps.setDate(2, start);
		ResultSet rs = ps.executeQuery();
		return rs;
	}

}
