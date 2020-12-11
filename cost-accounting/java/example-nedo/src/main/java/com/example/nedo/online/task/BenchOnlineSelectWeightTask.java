package com.example.nedo.online.task;

import java.util.List;

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
 * 重量の照会
 */
public class BenchOnlineSelectWeightTask extends BenchOnlineTask {

	public BenchOnlineSelectWeightTask() {
		super("weight");
	}

	@Override
	public void execute() {
		logStart("factory=%d, date=%s", factoryId, date);

		tm.required(() -> {
			int productId = selectRandomItemId();

			logTarget("factory=%d, date=%s, product=%d", factoryId, date, productId);
			executeMain(productId);
		});

		logEnd("factory=%d, date=%s", factoryId, date);
	}

	protected int selectRandomItemId() {
		List<Integer> list = itemManufacturingMasterDao.selectIdByFactory(factoryId, date);
		int i = random.nextInt(list.size());
		return list.get(i);
	}

	protected void executeMain(int productId) {
		List<ResultTable> list = resultTableDao.selectByProductId(factoryId, date, productId);

		FactoryMaster factory = factoryMasterDao.selectById(factoryId);

		ItemMaster product = itemMasterDao.selectById(productId, date);
		for (ResultTable result : list) {
			ItemMaster item = itemMasterDao.selectById(result.getRIId(), date);
			console("factory=%s, product=%s, parent=%d, item=%s, weight=%s %s, ratio=%.3f", factory.getFName(),
					product.getIName(), result.getRParentIId(), item.getIName(), result.getRWeightTotal(),
					result.getRWeightTotalUnit(), result.getRWeightRatio());
		}
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineSelectWeightTask task = new BenchOnlineSelectWeightTask();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		task.setDao(tm, new ItemManufacturingMasterDaoImpl(), new FactoryMasterDaoImpl(), new ItemMasterDaoImpl(), null,
				null, new ResultTableDaoImpl());

		task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

		task.execute();

//		tm.required(() -> {
//			task.executeMain(322);
//		});
	}
}
