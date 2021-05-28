package com.example.nedo.online.task;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.seasar.doma.jdbc.UniqueConstraintException;

import com.example.nedo.init.InitialData;
import com.example.nedo.init.InitialData03ItemMaster;
import com.example.nedo.init.InitialData04ItemManufacturingMaster;
import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

/**
 * 新規開発商品の追加
 */
public class BenchOnlineNewItemTask extends BenchOnlineTask {

	public BenchOnlineNewItemTask() {
		super("new-item");
	}

	@Override
	protected boolean execute1() {
		ItemMaster item;
		for (;;) {
			ItemMaster i = dbManager.execute(() -> {
				return executeMain();
			});
			if (i != null) {
				item = i;
				break;
			}
		}
		dbManager.execute(() -> {
			executeMain2(item);
		});
		return true;
	}

	protected ItemMaster executeMain() {
		InitialData03ItemMaster initialData = new InitialData03ItemMaster(date);

		ItemMaster item;
		for (;;) {
			item = insertItemMaster(initialData);
			switch (1) {
			case 1:
				if (item == null) {
					// 別コネクションでリトライ
					return null;
				}
				break;
			default:
				if (item == null) {
					// 同一コネクション内でリトライ（PostgreSQLだと例外発生時に同一コネクションでSQLを発行するとエラーになる）
					continue;
				}
				break;
			}
			break;
		}

		logTarget("product=%s", item.getIName());

		ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
		List<Integer> workList = itemMasterDao.selectIdByType(date, ItemType.WORK_IN_PROCESS);
		int s = random.random(1, InitialData03ItemMaster.PRODUCT_TREE_SIZE);
		Set<Integer> workSet = new HashSet<>(s);
		for (int i = 0; i < s; i++) {
			Integer id = initialData.getRandomAndRemove(i, workList);
			workSet.add(id);
		}

		initialData.insertItemConstructionMasterProduct(item.getIId(), workSet,
				dbManager.getItemConstructionMasterDao());

		return item;
	}

	private ItemMaster insertItemMaster(InitialData03ItemMaster initialData) {
		ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
		int newId = itemMasterDao.selectMaxId();

		ItemMaster entity = initialData.newItemMasterProduct(newId);
		entity.setEffectiveDate(date);
		LocalDate endDate = initialData.getRandomExpiredDate(newId, date);
		entity.setExpiredDate(endDate);

		try {
			itemMasterDao.insert(entity);
		} catch (UniqueConstraintException e) {
			return null;
		} catch (Exception e) {
			e.printStackTrace();
			throw e;
		}

		return entity;
	}

	protected void executeMain2(ItemMaster item) {
		InitialData04ItemManufacturingMaster initialData = new InitialData04ItemManufacturingMaster(1, date);

		ItemManufacturingMaster entity = initialData.newItemManufacturingMaster(factoryId, item.getIId());
		ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
		itemManufacturingMasterDao.insert(entity);
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineNewItemTask task = new BenchOnlineNewItemTask();

		try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
			task.setDao(manager);

			task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

			task.execute();
		}
	}
}
