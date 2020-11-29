package com.example.nedo.online.task;

import java.math.BigDecimal;
import java.util.List;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.InitialData;
import com.example.nedo.init.MeasurementUtil;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.CostMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.jdbc.doma2.entity.CostMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

/**
 * 原価の変更
 */
public class BenchOnlineUpdateCostTask extends BenchOnlineTask {

	public BenchOnlineUpdateCostTask() {
		super("update-cost");
	}

	@Override
	public void execute() {
		logStart("factory=%d, date=%s", factoryId, date);

		tm.required(() -> {
			CostMaster cost = selectRandomItem();

			int pattern = random.random(0, 1);
			if (pattern == 0) {
				executeIncrease(cost);
			} else {
				executeDecrease(cost);
			}
		});

		logEnd("factory=%d, date=%s", factoryId, date);
	}

	private CostMaster selectRandomItem() {
		List<CostMaster> list = costMasterDao.selectByFactory(factoryId);
		int i = random.nextInt(list.size());
		return list.get(i);
	}

	private static final BigDecimal Q_START = new BigDecimal("1.0");
	private static final BigDecimal Q_END = new BigDecimal("10.0");
	private static final BigDecimal Q_MULTI = new BigDecimal("100");
	private static final BigDecimal A_START = new BigDecimal("0.90");
	private static final BigDecimal A_END = new BigDecimal("1.10");

	protected void executeIncrease(CostMaster cost) {
		logTarget("increase product=%d", cost.getCIId());

		// 増やす在庫数
		BigDecimal quantity = random.random(Q_START, Q_END).multiply(Q_MULTI);

		// 増やす金額
		BigDecimal amount;
		{
			ItemMaster item = itemMasterDao.selectById(cost.getCIId(), date);
			BigDecimal price = MeasurementUtil.convertPriceUnit(item.getIPrice(), item.getIPriceUnit(),
					cost.getCStockUnit());
			amount = price.multiply(random.random(A_START, A_END)).multiply(quantity);
		}

		// 更新
		int r = costMasterDao.updateIncrease(cost, quantity, amount);
		assert r == 1;
	}

	protected void executeDecrease(CostMaster cost) {
		logTarget("decrease product=%d", cost.getCIId());

		// 減らす在庫数
		BigDecimal quantity = random.random(Q_START, Q_END).multiply(Q_MULTI);

		BigDecimal after = cost.getCStockQuantity().subtract(quantity);
		if (after.compareTo(BigDecimal.ZERO) <= 0) {
			executeIncrease(cost);
			return;
		}

		// 更新
		int r = costMasterDao.updateDecrease(cost, quantity);
		assert r == 1;
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineUpdateCostTask task = new BenchOnlineUpdateCostTask();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		task.setDao(tm, new ItemManufacturingMasterDaoImpl(), new FactoryMasterDaoImpl(), new ItemMasterDaoImpl(),
				new ItemConstructionMasterDaoImpl(), new CostMasterDaoImpl(), new ResultTableDaoImpl());

		task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

		task.execute();
	}
}
