package com.example.nedo.online;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.BenchRandom;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.CostMasterDao;
import com.example.nedo.jdbc.doma2.dao.CostMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.online.task.BenchOnlineInsertItemTask;
import com.example.nedo.online.task.BenchOnlineSelectCostTask;
import com.example.nedo.online.task.BenchOnlineSelectRequiredQuantityTask;
import com.example.nedo.online.task.BenchOnlineSelectWeightTask;
import com.example.nedo.online.task.BenchOnlineTask;
import com.example.nedo.online.task.BenchOnlineUpdateCostTask;
import com.example.nedo.online.task.BenchOnlineUpdateManufacturingTask;
import com.example.nedo.online.task.BenchOnlineUpdateMaterialTask;

public class BenchOnlineThread implements Runnable, Callable<Void> {

	private final List<Integer> factoryList;
	private final LocalDate date;
	private final List<BenchOnlineTask> taskList = new ArrayList<>();

	private final BenchRandom random = new BenchRandom();

	public BenchOnlineThread(List<Integer> factoryList, LocalDate date) {
		this.factoryList = factoryList;
		this.date = date;

		taskList.add(new BenchOnlineInsertItemTask());
		taskList.add(new BenchOnlineUpdateManufacturingTask());
		taskList.add(new BenchOnlineUpdateMaterialTask());
		taskList.add(new BenchOnlineUpdateCostTask());

		taskList.add(new BenchOnlineSelectWeightTask());
		taskList.add(new BenchOnlineSelectRequiredQuantityTask());
		taskList.add(new BenchOnlineSelectCostTask());
	}

	@Override
	public void run() {
		{
			TransactionManager tm = AppConfig.singleton().getTransactionManager();
			ItemManufacturingMasterDao itemManufacturingMasterDao = new ItemManufacturingMasterDaoImpl();
			FactoryMasterDao factoryMasterDao = new FactoryMasterDaoImpl();
			ItemMasterDao itemMasterDao = new ItemMasterDaoImpl();
			ItemConstructionMasterDao ItemConstructionMasterDao = new ItemConstructionMasterDaoImpl();
			CostMasterDao costMasterDao = new CostMasterDaoImpl();
			ResultTableDao resultTableDao = new ResultTableDaoImpl();
			for (BenchOnlineTask task : taskList) {
				task.setDao(tm, itemManufacturingMasterDao, factoryMasterDao, itemMasterDao, ItemConstructionMasterDao,
						costMasterDao, resultTableDao);
			}
		}

		for (;;) {
			int factoryId = factoryList.get(random.nextInt(factoryList.size()));

			// TODO タスク選択は均等でなくする
			BenchOnlineTask task = taskList.get(random.nextInt(taskList.size()));
			task.initialize(factoryId, date);

			task.execute();

			// TODO 終了させる方法
			if (Thread.interrupted()) {
				break;
			}

			// TODO sleep入れる？
		}
		
		System.out.println("thread-end");
	}

	@Override
	public Void call() throws Exception {
		run();
		return null;
	}
}
