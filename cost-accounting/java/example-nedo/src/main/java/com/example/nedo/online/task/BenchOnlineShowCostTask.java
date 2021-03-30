package com.example.nedo.online.task;

import java.util.stream.Stream;

import com.example.nedo.init.InitialData;
import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.entity.FactoryMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;
import com.example.nedo.jdbc.doma2.entity.ResultTable;

/**
 * 原価の照会
 */
public class BenchOnlineShowCostTask extends BenchOnlineTask {

	public BenchOnlineShowCostTask() {
		super("show-cost");
	}

	@Override
	protected void execute1() {
		dbManager.execute(() -> {
			executeMain();
		});
	}

	protected void executeMain() {
		FactoryMasterDao factoryMasterDao = dbManager.getFactoryMasterDao();
		FactoryMaster factory = factoryMasterDao.selectById(factoryId);

		ResultTableDao resultTableDao = dbManager.getResultTableDao();
		try (Stream<ResultTable> stream = resultTableDao.selectCost(factoryId, date)) {
			stream.forEach(result -> {
				ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
				ItemMaster item = itemMasterDao.selectById(result.getRIId(), date);
				console("factory=%s, product=%s, total=%s, quantity=%s, cost=%s", factory.getFName(), item.getIName(),
						result.getRTotalManufacturingCost(), result.getRManufacturingQuantity(),
						result.getRManufacturingCost());
			});
		}
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineShowCostTask task = new BenchOnlineShowCostTask();

		try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
			task.setDao(manager);

			task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

			task.execute();
		}
	}
}
