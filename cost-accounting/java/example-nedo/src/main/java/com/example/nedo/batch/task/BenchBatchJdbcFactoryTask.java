package com.example.nedo.batch.task;

import static com.example.nedo.batch.task.JdbcTaskUtil.*;
import static com.example.nedo.batch.task.JdbcTaskUtil.setDate;
import static com.example.nedo.batch.task.JdbcTaskUtil.setInt;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.time.LocalDate;

import org.seasar.doma.jdbc.tx.LocalTransaction;
import org.seasar.doma.jdbc.tx.LocalTransactionDataSource;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

public class BenchBatchJdbcFactoryTask extends BenchBatchFactoryTask {

	private Connection connection;

	public BenchBatchJdbcFactoryTask(int commitRatio, LocalDate batchDate, int factoryId) {
		super(commitRatio, batchDate, factoryId);
	}

	@Override
	public void run() {
		try (Connection c = getConnection()) {
			this.connection = c;
			try {
				BenchBatchItemTask itemTask = newBenchBatchItemTask();

				deleteResult();

				int count = 0;
				try (PreparedStatement ps = selectManufacturingItem()) {
					try (ResultSet rs = ps.executeQuery()) {
						while (rs.next()) {
							count++;
							ItemManufacturingMaster item = newItemManufacturingMaster(rs);
							itemTask.execute(item);
						}
					}
				}

				commitOrRollback(count);
			} catch (Throwable t) {
				try {
					doRollback();
				} catch (Throwable s) {
					t.addSuppressed(s);
				}
				throw t;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private Connection getConnection() {
		LocalTransactionDataSource dataSource = AppConfig.singleton().getDataSource();
		LocalTransaction transaction = dataSource.getLocalTransaction(AppConfig.singleton().getJdbcLogger());
		transaction.begin();
		try {
			return dataSource.getConnection();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public BenchBatchItemTask newBenchBatchItemTask() {
		BenchBatchJdbcItemTask itemTask = new BenchBatchJdbcItemTask(batchDate);
		itemTask.setConnection(connection);
		return itemTask;
	}

	private void deleteResult() {
		// resultTableDao.deleteByFactory(factoryId, batchDate);
		String sql = "delete from " + ResultTableDao.TABLE_NAME + " where r_f_id = ? and "
				+ ResultTableDao.PS_COND_DATE;
		try (PreparedStatement ps = connection.prepareStatement(sql)) {
			int i = 1;
			setInt(ps, i++, factoryId);
			setDate(ps, i++, batchDate);

			ps.executeUpdate();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	private PreparedStatement selectManufacturingItem() throws SQLException {
		// return itemManufacturingMasterDao.selectByFactory(factoryId, batchDate);
		String sql = "select * from " + ItemManufacturingMasterDao.TABLE_NAME + " where im_f_id = ? and "
				+ ItemManufacturingMasterDao.PS_COND_DATE;
		PreparedStatement ps = connection.prepareStatement(sql);
		try {
			int i = 1;
			setInt(ps, i++, factoryId);
			setDate(ps, i++, batchDate);
		} catch (Throwable t) {
			try {
				ps.close();
			} catch (Throwable s) {
				t.addSuppressed(s);
			}
			throw t;
		}
		return ps;
	}

	private ItemManufacturingMaster newItemManufacturingMaster(ResultSet rs) throws SQLException {
		ItemManufacturingMaster entity = new ItemManufacturingMaster();

		entity.setImFId(getInt(rs, "im_f_id"));
		entity.setImIId(getInt(rs, "im_i_id"));
		entity.setImEffectiveDate(getDate(rs, "im_effective_date"));
		entity.setImExpiredDate(getDate(rs, "im_expired_date"));
		entity.setImManufacturingQuantity(getBigInt(rs, "im_manufacturing_quantity"));

		return entity;
	}

	@Override
	protected void doCommit() {
		LocalTransactionDataSource dataSource = AppConfig.singleton().getDataSource();
		LocalTransaction transaction = dataSource.getLocalTransaction(AppConfig.singleton().getJdbcLogger());
		transaction.commit();
	}

	@Override
	protected void doRollback() {
		LocalTransactionDataSource dataSource = AppConfig.singleton().getDataSource();
		LocalTransaction transaction = dataSource.getLocalTransaction(AppConfig.singleton().getJdbcLogger());
		transaction.rollback();
	}
}
