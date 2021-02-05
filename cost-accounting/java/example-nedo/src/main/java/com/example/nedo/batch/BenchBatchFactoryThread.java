package com.example.nedo.batch;

import java.time.LocalDate;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.BenchRandom;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.CostMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

public class BenchBatchFactoryThread implements Runnable, Callable<Void> {

	private final BenchBatch batch;
	private final LocalDate batchDate;
	private final int factoryId;

	private final BenchRandom random = new BenchRandom();

	public BenchBatchFactoryThread(BenchBatch batch, LocalDate batchDate, int factoryId) {
		this.batch = batch;
		this.batchDate = batchDate;
		this.factoryId = factoryId;
	}

	@Override
	public void run() {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		ResultTableDao resultTableDao = new ResultTableDaoImpl();

		BenchBatchItemTask itemTask = new BenchBatchItemTask(batchDate);
		itemTask.setDao(new ItemConstructionMasterDaoImpl(), new ItemMasterDaoImpl(), new CostMasterDaoImpl(),
				resultTableDao);

		tm.required(() -> {
			deleteResult(resultTableDao);

			int[] count = { 0 };
			try (Stream<ItemManufacturingMaster> stream = selectMakeItem()) {
				stream.forEach(item -> {
					count[0]++;
					itemTask.execute(item);
				});
			}

			batch.commitOrRollback(tm, batchDate, factoryId, count[0], random);
		});
	}

	@Override
	public Void call() {
		run();
		return null;
	}

	private void deleteResult(ResultTableDao dao) {
		dao.deleteByFactory(factoryId, batchDate);
	}

	private Stream<ItemManufacturingMaster> selectMakeItem() {
		ItemManufacturingMasterDao dao = new ItemManufacturingMasterDaoImpl();

		return dao.selectByFactory(factoryId, batchDate);
	}
}
