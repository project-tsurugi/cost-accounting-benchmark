package com.example.nedo.batch.task;

import java.time.LocalDate;
import java.util.concurrent.Callable;

import com.example.nedo.init.BenchRandom;

// 1 thread
public abstract class BenchBatchFactoryTask implements Runnable, Callable<Void> {

	protected final int commitRatio;
	protected final LocalDate batchDate;
	protected final int factoryId;

	protected final BenchRandom random = new BenchRandom();

	private int commitCount = 0;
	private int rollbackCount = 0;

	public BenchBatchFactoryTask(int commitRatio, LocalDate batchDate, int factoryId) {
		this.commitRatio = commitRatio;
		this.batchDate = batchDate;
		this.factoryId = factoryId;
	}

	@Override
	public final Void call() {
		run();
		return null;
	}

	public abstract BenchBatchItemTask newBenchBatchItemTask();

	public void commitOrRollback(int count) {
		int n = random.random(0, 99);
		if (n < commitRatio) {
			doCommit();
			commitCount += count;
			System.out.printf("commit (%s, %d), count=%d\n", batchDate, factoryId, count);
		} else {
			doRollback();
			rollbackCount += count;
			System.out.printf("rollback (%s, %d), count=%d\n", batchDate, factoryId, count);
		}
	}

	protected abstract void doCommit();

	protected abstract void doRollback();

	public final int getCommitCount() {
		return commitCount;
	}

	public final int getRollbackCount() {
		return rollbackCount;
	}
}
