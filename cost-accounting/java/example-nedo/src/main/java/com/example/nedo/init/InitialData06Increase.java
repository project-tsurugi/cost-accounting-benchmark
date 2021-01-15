package com.example.nedo.init;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ForkJoinTask;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMasterIds;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMasterIds;

@SuppressWarnings("serial")
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
		List<ForkJoinTask<?>> taskList = new ArrayList<>();
		increaseItemConstructionMaster(taskList, new ItemConstructionMasterDaoImpl());
		increaseItemManufacturingMaster(taskList, new ItemManufacturingMasterDaoImpl());
		System.out.println("fork end " + LocalDateTime.now());

		for (ForkJoinTask<?> task : taskList) {
			task.join();
		}
	}

	private static final int TASK_THRESHOLD = DaoSplitTask.TASK_THRESHOLD;

	private void increaseItemConstructionMaster(List<ForkJoinTask<?>> taskList, ItemConstructionMasterDao dao) {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		List<ItemConstructionMasterIds> list = tm.required(() -> dao.selectIds(batchDate));

		IncreaseItemConstructionMasterTask task = new IncreaseItemConstructionMasterTask(dao);

		int increaseSize = (int) (list.size() * 0.25); // 1.25倍に増幅する
		for (int i = 0; i < increaseSize; i++) {
			ItemConstructionMasterIds ids = getRandomAndRemove(i, list);
			task.add(ids);
			if (task.size() >= TASK_THRESHOLD) {
				task.fork();
				taskList.add(task);
				task = new IncreaseItemConstructionMasterTask(dao);
			}
		}

		task.fork();
		taskList.add(task);
	}

	private class IncreaseItemConstructionMasterTask extends DaoListTask<ItemConstructionMasterIds> {
		private final ItemConstructionMasterDao dao;

		public IncreaseItemConstructionMasterTask(ItemConstructionMasterDao dao) {
			this.dao = dao;
		}

		@Override
		protected void execute(ItemConstructionMasterIds ids) {
			ItemConstructionMaster src = dao.selectById(ids.getIcParentIId(), ids.getIcIId(), batchDate);

			ItemConstructionMaster entity = src.clone();
			int seed = entity.getIcParentIId() + entity.getIcIId();
			if (random.prandom(seed, 0, 1) == 0) {
				initializePrevStartEndDate(seed + 1, src, entity);
			} else {
				initializeNextStartEndDate(seed + 1, src, entity);
			}
			InitialData03ItemMaster.initializeItemConstructionMasterRandom(random, entity);

			dao.insert(entity);
		}
	}

	private void increaseItemManufacturingMaster(List<ForkJoinTask<?>> taskList, ItemManufacturingMasterDao dao) {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		List<ItemManufacturingMasterIds> list = tm.required(() -> dao.selectIds(batchDate));

		IncreaseItemManufacturingMasterTask task = new IncreaseItemManufacturingMasterTask(dao);

		int increaseSize = (int) (list.size() * 0.6); // 1.6倍に増幅する
		for (int i = 0; i < increaseSize; i++) {
			ItemManufacturingMasterIds ids = getRandomAndRemove(i, list);
			task.add(ids);
			if (task.size() >= TASK_THRESHOLD) {
				task.fork();
				taskList.add(task);
				task = new IncreaseItemManufacturingMasterTask(dao);
			}
		}

		task.fork();
		taskList.add(task);
	}

	private class IncreaseItemManufacturingMasterTask extends DaoListTask<ItemManufacturingMasterIds> {
		private final ItemManufacturingMasterDao dao;

		public IncreaseItemManufacturingMasterTask(ItemManufacturingMasterDao dao) {
			this.dao = dao;
		}

		@Override
		protected void execute(ItemManufacturingMasterIds ids) {
			ItemManufacturingMaster src = dao.selectById(ids.getImFId(), ids.getImIId(), batchDate);

			ItemManufacturingMaster entity = src.clone();
			int seed = entity.getImFId() + entity.getImIId();
			if (random.prandom(seed, 0, 1) == 0) {
				initializePrevStartEndDate(seed + 1, src, entity);
			} else {
				initializeNextStartEndDate(seed + 1, src, entity);
			}
			InitialData04ItemManufacturingMaster.initializeItemManufacturingMasterRandom(random, entity);

			dao.insert(entity);
		}
	}
}
