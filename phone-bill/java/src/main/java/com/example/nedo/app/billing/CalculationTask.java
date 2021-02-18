package com.example.nedo.app.billing;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;

/**
 * @author umega
 *
 */
public class CalculationTask implements Callable<Exception> {
    private static final Logger LOG = LoggerFactory.getLogger(CalculationTask.class);

    private String batchExecId;


	/**
	 * 計算対象が格納されているQueue
	 */
	private BlockingQueue<CalculationTarget> queue;


	/**
	 * DBConnection;
	 */
	private Connection conn;


	/**
	 * コンストラクタ
	 *
	 * @param queue
	 * @param conn
	 */
	public CalculationTask(BlockingQueue<CalculationTarget> queue, Connection conn, String batchExecId) {
		this.queue = queue;
		this.conn = conn;
		this.batchExecId = batchExecId;
	}


	@Override
	public Exception call() throws Exception {
		try {
			for(;;) {
				CalculationTarget target;
				try {
					target = queue.take();
				} catch (InterruptedException e) {
					LOG.debug("InterruptedException caught and continue taking calculation_target", e);
					continue;
				}
				if (target.isEndOfTask()) {
					return null;
				}
				doCalc(target);
			}
		} catch (Exception e) {
			return e;
		}
	}

	/**
	 * 料金計算のメインロジック
	 *
	 * @param target
	 * @throws SQLException
	 */
	private void doCalc(CalculationTarget target) throws SQLException {
		Contract contract = target.getContract();
		Date start = target.getStart();
		Date end = target.getEnd();
		CallChargeCalculator callChargeCalculator = target.getCallChargeCalculator();
		BillingCalculator billingCalculator = target.getBillingCalculator();


		try (ResultSet historyResultSet = getHistoryResultSet(contract, start, end);
				Statement stmt = historyResultSet.getStatement();) {
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

	/**
	 * historyテーブルから指定のcontractの料金計算対象のレコードを操作するためのResultSetを取り出す
	 *
	 * @param contract 検索対象の契約
	 * @param start 検索対象期間の開始日
	 * @param end 検索対象期間の最終日
	 * @return
	 * @throws SQLException
	 */
	private ResultSet getHistoryResultSet(Contract contract, Date start, Date end)
			throws SQLException {
		String sql = "select caller_phone_number, start_time, time_secs, charge"
				+ " from history "
				+ "where start_time >= ? and start_time < ?"
				+ " and ((caller_phone_number = ? and payment_categorty = 'C') "
				+ "  or (recipient_phone_number = ? and payment_categorty = 'R'))"
				+ " and df = 0";

		PreparedStatement ps = conn.prepareStatement(sql,ResultSet.TYPE_FORWARD_ONLY	, ResultSet.CONCUR_UPDATABLE);
		ps.setDate(1, start);
		ps.setDate(2, DBUtils.nextDate(end));
		ps.setString(3, contract.phoneNumber);
		ps.setString(4, contract.phoneNumber);
		ResultSet rs = ps.executeQuery();
		return rs;
	}

	/**
	 * Billingテーブルを更新する
	 *
	 * @param conn
	 * @param contract
	 * @param billingCalculator
	 * @param targetMonth
	 * @throws SQLException
	 */
	private void updateBilling(Connection conn, Contract contract, BillingCalculator billingCalculator,
			Date targetMonth) throws SQLException {
		String sql = "insert into billing("
				+ "phone_number, target_month, basic_charge, metered_charge, billing_amount, batch_exec_id)"
				+ " values(?, ?, ?, ?, ?, ?)";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setString(1, contract.phoneNumber);
			ps.setDate(2, targetMonth);
			ps.setInt(3, billingCalculator.getBasicCharge());
			ps.setInt(4, billingCalculator.getMeteredCharge());
			ps.setInt(5, billingCalculator.getBillingAmount());
			ps.setString(6, batchExecId);
			ps.executeUpdate();
		}
	}



}