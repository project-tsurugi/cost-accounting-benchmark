package com.example.nedo.online;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import com.example.nedo.BenchConst;
import com.example.nedo.init.BenchRandom;
import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.online.task.BenchOnlineNewItemTask;
import com.example.nedo.online.task.BenchOnlineShowCostTask;
import com.example.nedo.online.task.BenchOnlineShowQuantityTask;
import com.example.nedo.online.task.BenchOnlineShowWeightTask;
import com.example.nedo.online.task.BenchOnlineTask;
import com.example.nedo.online.task.BenchOnlineUpdateCostTask;
import com.example.nedo.online.task.BenchOnlineUpdateManufacturingTask;
import com.example.nedo.online.task.BenchOnlineUpdateMaterialTask;

public class BenchOnlineThread implements Runnable, Callable<Void> {

	private final CostBenchDbManager dbManager;
	private final List<Integer> factoryList;
	private final LocalDate date;
	private final List<BenchOnlineTask> taskList = new ArrayList<>();
	private final NavigableMap<Integer, BenchOnlineTask> taskRatioMap = new TreeMap<>();
	private final int taskRatioMax;

	private final BenchRandom random = new BenchRandom();

	public BenchOnlineThread(CostBenchDbManager dbManager, List<Integer> factoryList, LocalDate date) {
		this.dbManager = dbManager;
		this.factoryList = factoryList;
		this.date = date;

		taskList.add(new BenchOnlineNewItemTask());
		taskList.add(new BenchOnlineUpdateManufacturingTask());
		taskList.add(new BenchOnlineUpdateMaterialTask());
		taskList.add(new BenchOnlineUpdateCostTask());

		taskList.add(new BenchOnlineShowWeightTask());
		taskList.add(new BenchOnlineShowQuantityTask());
		taskList.add(new BenchOnlineShowCostTask());

		this.taskRatioMax = initializeTaskRatio(taskList);
	}

	private int initializeTaskRatio(List<BenchOnlineTask> taskList) {
		int sum = 0;
		for (BenchOnlineTask task : taskList) {
			String title = task.getTitle();
			int ratio = BenchConst.onlineTaskRatio(title);
			if (ratio > 0) {
				sum += ratio;
				taskRatioMap.put(sum, task);
			}
		}
		return sum;
	}

	@Override
	public void run() {
		for (BenchOnlineTask task : taskList) {
			task.setDao(dbManager);
		}

		for (;;) {
			int factoryId = factoryList.get(random.nextInt(factoryList.size()));

			BenchOnlineTask task = getTaskRandom();
			task.initialize(factoryId, date);

			task.execute();

			// TODO 終了させる方法
			if (Thread.interrupted()) {
				break;
			}

			long sleepTime = task.getSleepTime();
			if (sleepTime > 0) {
				try {
					Thread.sleep(sleepTime);
				} catch (InterruptedException e) {
					break;
				}
			}
		}

		System.out.println("thread-end");
	}

	private BenchOnlineTask getTaskRandom() {
		int i = random.nextInt(taskRatioMax);
		BenchOnlineTask task = taskRatioMap.higherEntry(i).getValue();
		return task;
	}

	@Override
	public Void call() throws Exception {
		run();
		return null;
	}
}
