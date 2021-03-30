package com.example.nedo.init.util;

import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;

import com.example.nedo.jdbc.CostBenchDbManager;

@SuppressWarnings("serial")
public abstract class DaoSplitTask extends RecursiveAction {
	public static final int TASK_THRESHOLD = 10000;

	private final CostBenchDbManager dbManager;
	private final int startId;
	private final int endId;

	public DaoSplitTask(CostBenchDbManager dbManager, int startId, int endId) {
		this.dbManager = dbManager;
		this.startId = startId;
		this.endId = endId;
	}

	@Override
	protected final void compute() {
		int size = endId - startId;
		if (size > TASK_THRESHOLD) {
			int middleId = startId + size / 2;
			ForkJoinTask<Void> task1 = createTask(startId, middleId).fork();
			ForkJoinTask<Void> task2 = createTask(middleId, endId).fork();
			task1.join();
			task2.join();
		} else {
			dbManager.execute(() -> {
				for (int iId = startId; iId < endId; iId++) {
					execute(iId);
				}
			});
		}
	}

	protected abstract DaoSplitTask createTask(int startId, int endId);

	protected abstract void execute(int iId);
}
