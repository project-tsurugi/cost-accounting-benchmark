package com.example.nedo.online.task;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;

import com.example.nedo.init.InitialData;
import com.example.nedo.init.InitialData04ItemManufacturingMaster;
import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

/**
 * 生産数の変更
 */
public class BenchOnlineUpdateManufacturingTask extends BenchOnlineTask {

	public BenchOnlineUpdateManufacturingTask() {
		super("update-manufacturing");
	}

	@Override
	protected boolean execute1() {
		return dbManager.execute(() -> {
			int productId = selectRandomItemId();
			if (productId < 0) {
				return false;
			}

			logTarget("factory=%d, date=%s, product=%d", factoryId, date, productId);
			executeMain(productId);
			return true;
		});
	}

	protected void executeMain(int productId) {
		int newQuantity = random.random(0, 500) * 100;

		ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
		ItemManufacturingMaster entity = itemManufacturingMasterDao.selectByIdForUpdate(factoryId, productId, date);
		if (entity == null) {
			InitialData04ItemManufacturingMaster initialData = new InitialData04ItemManufacturingMaster(1, date);
			entity = initialData.newItemManufacturingMaster(factoryId, productId);
			entity.setEffectiveDate(date);
			{
				List<ItemManufacturingMaster> list = itemManufacturingMasterDao.selectByIdFuture(productId, productId,
						date);
				if (!list.isEmpty()) {
					ItemManufacturingMaster min = list.get(0);
					entity.setExpiredDate(min.getEffectiveDate().minusDays(1));
				} else {
					entity.setExpiredDate(LocalDate.of(9999, 12, 31));
				}
			}
			entity.setImManufacturingQuantity(BigInteger.valueOf(newQuantity));
			itemManufacturingMasterDao.insert(entity);
		} else {
			entity.setImManufacturingQuantity(BigInteger.valueOf(newQuantity));
			itemManufacturingMasterDao.update(entity);
		}
	}

	protected int selectRandomItemId() {
		ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
		List<Integer> list = itemMasterDao.selectIdByType(date, ItemType.PRODUCT);
		if (list.isEmpty()) {
			return -1;
		}
		int i = random.nextInt(list.size());
		return list.get(i);
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineUpdateManufacturingTask task = new BenchOnlineUpdateManufacturingTask();

		try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
			task.setDao(manager);

			task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

			task.execute();
//			manager.execute(() -> {
//				task.executeMain(49666);
//			});
		}
	}
}
