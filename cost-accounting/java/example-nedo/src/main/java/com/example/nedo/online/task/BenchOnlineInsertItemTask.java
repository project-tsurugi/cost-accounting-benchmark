package com.example.nedo.online.task;

import java.time.LocalDate;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.seasar.doma.jdbc.UniqueConstraintException;
import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.InitialData;
import com.example.nedo.init.InitialData03ItemMaster;
import com.example.nedo.init.InitialData04ItemManufacturingMaster;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

/**
 * 新規開発商品の追加
 */
public class BenchOnlineInsertItemTask extends BenchOnlineTask {

	public BenchOnlineInsertItemTask() {
		super("new-item");
	}

	@Override
	public void execute() {
		logStart("factory=%d, date=%s", factoryId, date);

		ItemMaster item;
		for (;;) {
			ItemMaster i = tm.required(() -> {
				return executeMain();
			});
			if (i != null) {
				item = i;
				break;
			}
		}
		tm.required(() -> {
			executeMain2(item);
		});

		logEnd("factory=%d, date=%s", factoryId, date);
	}

	protected ItemMaster executeMain() {
		InitialData03ItemMaster initialData = new InitialData03ItemMaster(date);

		ItemMaster item;
		for (;;) {
			item = insertItemMaster(initialData);
			switch (2) {
			case 1:
				if (item == null) {
					return null;
				}
				break;
			default:
				if (item == null) {
					continue;
				}
				break;
			}
			break;
		}

		logTarget("product=%s", item.getIName());

		List<Integer> workList = itemMasterDao.selectIdByType(date, ItemType.WORK_IN_PROCESS);
		int s = random.random(1, InitialData03ItemMaster.PRODUCT_TREE_SIZE);
		Set<Integer> workSet = new HashSet<>(s);
		for (int i = 0; i < s; i++) {
			Integer id = initialData.getRandomAndRemove(i, workList);
			workSet.add(id);
		}

		initialData.insertItemConstructionMasterProduct(item.getIId(), workSet, itemCostructionMasterDao);

		return item;
	}

	private ItemMaster insertItemMaster(InitialData03ItemMaster initialData) {
		int newId = itemMasterDao.selectMaxId();

		ItemMaster entity = initialData.newItemMasterProduct(newId);
		entity.setEffectiveDate(date);
		LocalDate endDate = initialData.getRandomExpiredDate(date);
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
		InitialData04ItemManufacturingMaster initialData = new InitialData04ItemManufacturingMaster(date);

		ItemManufacturingMaster entity = initialData.newItemManufacturingMaster(factoryId, item.getIId());
		itemManufacturingMasterDao.insert(entity);
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineInsertItemTask task = new BenchOnlineInsertItemTask();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		task.setDao(tm, new ItemManufacturingMasterDaoImpl(), new FactoryMasterDaoImpl(), new ItemMasterDaoImpl(),
				new ItemConstructionMasterDaoImpl(), null, new ResultTableDaoImpl());

		task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

		task.execute();
	}
}
