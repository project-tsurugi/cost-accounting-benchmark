package com.example.nedo.batch.task;

import java.time.LocalDate;
import java.util.stream.Stream;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.CostMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

public class BenchBatchDoma2FactoryTask extends BenchBatchFactoryTask {

	private final ResultTableDao resultTableDao = new ResultTableDaoImpl();

	public BenchBatchDoma2FactoryTask(int commitRatio, LocalDate batchDate, int factoryId) {
		super(commitRatio, batchDate, factoryId);
	}

	@Override
	public void run() {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		BenchBatchItemTask itemTask = newBenchBatchItemTask();

		tm.required(() -> {
			deleteResult();

			int[] count = { 0 };
			try (Stream<ItemManufacturingMaster> stream = selectMakeItem()) {
				stream.forEach(item -> {
					count[0]++;
					itemTask.execute(item);
				});
			}

			commitOrRollback(count[0]);
		});
	}

	@Override
	public BenchBatchItemTask newBenchBatchItemTask() {
		BenchBatchDoma2ItemTask itemTask = new BenchBatchDoma2ItemTask(batchDate);
		itemTask.setDao(new ItemConstructionMasterDaoImpl(), new ItemMasterDaoImpl(), new CostMasterDaoImpl(),
				resultTableDao);
		return itemTask;
	}

	private void deleteResult() {
		resultTableDao.deleteByFactory(factoryId, batchDate);
	}

	private Stream<ItemManufacturingMaster> selectMakeItem() {
		ItemManufacturingMasterDao dao = new ItemManufacturingMasterDaoImpl();

		return dao.selectByFactory(factoryId, batchDate);
	}

	@Override
	protected void doCommit() {
		// do nothing
	}

	@Override
	protected void doRollback() {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		tm.setRollbackOnly();
	}
}
