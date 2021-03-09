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
 * 所要量の照会
 */
public class BenchOnlineShowQuantityTask extends BenchOnlineTask {

	public BenchOnlineShowQuantityTask() {
		super("show-quantity");
	}

	@Override
	protected void execute1() {
		tm.required(() -> {
			executeMain();
		});
	}

	protected void executeMain() {
		FactoryMaster factory = factoryMasterDao.selectById(factoryId);

		try (Stream<ResultTable> stream = resultTableDao.selectRequiredQuantity(factoryId, date)) {
			stream.forEach(result -> {
				ItemMaster item = itemMasterDao.selectById(result.getRIId(), date);
				console("factory=%s, item=%s, required_quantity=%s %s", factory.getFName(), item.getIName(),
						result.getRRequiredQuantity(), result.getRRequiredQuantityUnit());
			});
		}
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineShowQuantityTask task = new BenchOnlineShowQuantityTask();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		task.setDao(tm, new ItemManufacturingMasterDaoImpl(), new FactoryMasterDaoImpl(), new ItemMasterDaoImpl(), null,
				null, new ResultTableDaoImpl());

		task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

		task.execute();
	}
}
