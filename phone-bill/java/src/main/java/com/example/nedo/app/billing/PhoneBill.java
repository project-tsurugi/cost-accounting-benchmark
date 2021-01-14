package com.example.nedo.app.billing;

import java.sql.Connection;
import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.example.nedo.app.Config;
import com.example.nedo.app.ExecutableCommand;
import com.example.nedo.db.Contract;
import com.example.nedo.db.DBUtils;
import com.example.nedo.db.Duration;

public class PhoneBill implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(PhoneBill.class);


	public static void main(String[] args) throws Exception {
		PhoneBill phoneBill = new PhoneBill();
		phoneBill.execute(args);
	}

	@Override
	public void execute(String[] args) throws Exception {
		Config config = Config.getConfig(args);
		Duration d = toDuration(config.targetMonth);
		doCalc(config, d.start, d.end);
	}

	/**
	 * 指定の日付の一日から翌月の一日までのDurationを作成する
	 *
	 * @param date
	 * @return
	 */
	static Duration toDuration(Date date) {
		Calendar calendar = Calendar.getInstance();
		calendar.setTime(date);
		calendar.set(Calendar.DAY_OF_MONTH, 1);
		Date start = new Date(calendar.getTimeInMillis());
		calendar.add(Calendar.MONTH, 1);
		Date end = new Date(calendar.getTimeInMillis());
		return new Duration(start, end);
	}


	/**
	 * 料金計算のメイン処理
	 *
	 * @param config
	 * @param start
	 * @param end
	 * @throws Exception
	 */
	void doCalc(Config config, Date start, Date end) throws SQLException {
		String batchExecId = UUID.randomUUID().toString();
		int threadCount = 10; // TODO Configで指定可能にする
		boolean sharedConnection = false; // TODO Configで指定可能にする

		long startTime = System.currentTimeMillis();
		try (Connection conn = DBUtils.getConnection(config)) {
			List<Connection> connections = new ArrayList<Connection>(threadCount);
			connections.add(conn);
			// 契約毎の計算を行うスレッドを生成する
			ExecutorService service = Executors.newFixedThreadPool(threadCount);
			BlockingQueue<CalculationTarget> queue = new LinkedBlockingDeque<CalculationTarget>();
			Set<Future<Exception>> futures = new HashSet<>(threadCount);
			for(int i =0; i < threadCount; i++) {
				if (sharedConnection) {
					futures.add( service.submit(new CalculationTask(queue, conn, batchExecId)));
				} else {
					Connection newConnection = DBUtils.getConnection(config);
					connections.add(newConnection);
					futures.add( service.submit(new CalculationTask(queue, newConnection, batchExecId)));
				}
			}

			// Billingテーブルの計算対象月のレコードを削除する
			deleteTargetManthRecords(conn, start);
			// 計算対象の契約を取りだし、キューに入れる
			try (ResultSet contractResultSet = getContractResultSet(conn, start, end)) {
				while (contractResultSet.next()) {
					Contract contract = getContract(contractResultSet);
					LOG.debug(contract.toString());
					// TODO 契約内容に合致した、CallChargeCalculator, BillingCalculatorを生成するようにする。
					CallChargeCalculator callChargeCalculator = new SimpleCallChargeCalculator();
					BillingCalculator billingCalculator = new SimpleBillingCalculator();
					CalculationTarget target = new CalculationTarget(contract, billingCalculator,
							callChargeCalculator, start, end, false);
					putToQueue(queue, target);;
				}
			}

			// EndOfTaskをキューに入れる
			for (int i =0; i < threadCount; i++) {
				putToQueue(queue, CalculationTarget.getEndOfTask());
			}
			service.shutdown();
			cleanup(futures, connections);
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		String format = "Billings calculated in %,.3f sec ";
		LOG.info(String.format(format, elapsedTime / 1000d));
	}

	/**
	 * @param conn
	 * @param futures
	 * @param connections
	 * @throws SQLException
	 */
	private void cleanup(Set<Future<Exception>> futures, List<Connection> connections) throws SQLException {
		// TODO 適切なログを出力する
		boolean needRollback = false;
		while (!futures.isEmpty()) {
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				LOG.debug("InterruptedException caught and waiting service shutdown.", e);
			}
			Iterator<Future<Exception>> it = futures.iterator();
			while (it.hasNext()) {
				Exception e = null;
				{
					Future<Exception> future = it.next();
					if (future.isDone()) {
						it.remove();
						try {
							e = future.get(0, TimeUnit.SECONDS);
						} catch (InterruptedException e1) {
							continue;
						} catch (ExecutionException e1) {
							e = e1;
						} catch (TimeoutException e1) {
							continue;
						}
					}
				}
				if (e != null) {
					LOG.error("Exception cought", e);
					needRollback = true;
					for(Future<Exception> future: futures) {
						future.cancel(true);
					}
				}
			}
		}
		if (needRollback) {
			for (Connection c: connections) {
				c.rollback();
			}
		} else {
			for (Connection c: connections) {
				c.commit();
			}
		}
	}


	private void deleteTargetManthRecords(Connection conn, Date start) throws SQLException {
		String sql = "delete from billing where target_month = ?";
		try (PreparedStatement ps = conn.prepareStatement(sql)) {
			ps.setDate(1, start);
			ps.executeUpdate();
		}
	}

	/**
	 * queueにtargetを追加する。InterruptedException発生時は成功するまでリトライする。
	 *
	 * @param queue
	 * @param target
	 */
	private void putToQueue(BlockingQueue<CalculationTarget> queue, CalculationTarget target) {
		for(;;) {
			try {
				queue.put(target);
				break;
			} catch (InterruptedException e) {
				LOG.debug("InterruptedException caught and continue puting calculation_target", e);
			}
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
