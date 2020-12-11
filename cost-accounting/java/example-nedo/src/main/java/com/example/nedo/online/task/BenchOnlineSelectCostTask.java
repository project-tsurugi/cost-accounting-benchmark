package com.example.nedo.online.task;

import java.util.stream.Stream;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.InitialData;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.jdbc.doma2.entity.FactoryMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;
import com.example.nedo.jdbc.doma2.entity.ResultTable;

/**
 * 原価の照会
 */
public class BenchOnlineSelectCostTask extends BenchOnlineTask {

	public BenchOnlineSelectCostTask() {
		super("cost");
	}

	@Override
	public void execute() {
		logStart("factory=%d, date=%s", factoryId, date);

		tm.required(() -> {
			executeMain();
		});

		logEnd("factory=%d, date=%s", factoryId, date);
	}

	protected void executeMain() {
		FactoryMaster factory = factoryMasterDao.selectById(factoryId);

		try (Stream<ResultTable> stream = resultTableDao.selectCost(factoryId, date)) {
			stream.forEach(result -> {
				ItemMaster item = itemMasterDao.selectById(result.getRIId(), date);
				console("factory=%s, product=%s, total=%s, quantity=%s, cost=%s", factory.getFName(), item.getIName(),
						result.getRTotalManufacturingCost(), result.getRManufacturingQuantity(),
						result.getRManufacturingCost());
			});
		}
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineSelectCostTask task = new BenchOnlineSelectCostTask();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		task.setDao(tm, new ItemManufacturingMasterDaoImpl(), new FactoryMasterDaoImpl(), new ItemMasterDaoImpl(), null,
				null, new ResultTableDaoImpl());

		task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

		task.execute();
	}
}
