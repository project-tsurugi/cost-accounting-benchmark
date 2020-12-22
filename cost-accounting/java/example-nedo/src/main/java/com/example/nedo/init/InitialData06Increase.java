package com.example.nedo.init;

import java.time.LocalDate;
import java.util.List;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

public class InitialData06Increase extends InitialData {

	public static void main(String[] args) throws Exception {
		LocalDate batchDate = DEFAULT_BATCH_DATE;
		new InitialData06Increase(batchDate).main();
	}

	public InitialData06Increase(LocalDate batchDate) {
		super(batchDate);
	}

	private void main() {
		logStart();

		increaseMaster();

		logEnd();
	}

	private void increaseMaster() {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		tm.required(() -> {
			increaseItemConstructionMaster(new ItemConstructionMasterDaoImpl());
			increaseItemManufacturingMaster(new ItemManufacturingMasterDaoImpl());
		});
	}

	private void increaseItemConstructionMaster(ItemConstructionMasterDao dao) {
		List<ItemConstructionMaster> list = dao.selectAll(batchDate);

		int increaseSize = (int) (list.size() * 0.25); // 1.25倍に増幅する
		for (int i = 0; i < increaseSize; i++) {
			ItemConstructionMaster src = getRandomAndRemove(i, list);

			ItemConstructionMaster entity = src.clone();
			int seed = entity.getIcParentIId() + entity.getIcIId();
			if (random.prandom(seed + i, 0, 1) == 0) {
				initializePrevStartEndDate(src, entity);
			} else {
				initializeNextStartEndDate(src, entity);
			}
			InitialData03ItemMaster.initializeItemConstructionMasterRandom(random, entity);

			dao.insert(entity);
		}
	}

	private void increaseItemManufacturingMaster(ItemManufacturingMasterDao dao) {
		List<ItemManufacturingMaster> list = dao.selectAll(batchDate);

		int increaseSize = (int) (list.size() * 0.6); // 1.6倍に増幅する
		for (int i = 0; i < increaseSize; i++) {
			ItemManufacturingMaster src = getRandomAndRemove(i, list);

			ItemManufacturingMaster entity = src.clone();
			int seed = entity.getImFId() + entity.getImIId();
			if (random.prandom(seed + i, 0, 1) == 0) {
				initializePrevStartEndDate(src, entity);
			} else {
				initializeNextStartEndDate(src, entity);
			}
			InitialData04ItemManufacturingMaster.initializeItemManufacturingMasterRandom(random, entity);

			dao.insert(entity);
		}
	}
}
