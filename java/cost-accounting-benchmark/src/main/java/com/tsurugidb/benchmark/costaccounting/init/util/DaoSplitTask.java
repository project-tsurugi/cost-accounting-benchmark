package com.tsurugidb.benchmark.costaccounting.init.util;

import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

@SuppressWarnings("serial")
public abstract class DaoSplitTask extends RecursiveAction {
    public static final int TASK_THRESHOLD = BenchConst.initTaskThreshold();

    private final CostBenchDbManager dbManager;
    private final TgTmSetting setting;
    private final int startId;
    private final int endId;
    private final AtomicInteger insertCount = new AtomicInteger();

    public DaoSplitTask(CostBenchDbManager dbManager, TgTmSetting setting, int startId, int endId) {
        this.dbManager = dbManager;
        this.setting = setting;
        this.startId = startId;
        this.endId = endId;
    }

    @Override
    protected final void compute() {
        int size = endId - startId;
        if (size > TASK_THRESHOLD) {
            int middleId = startId + size / 2;
            DaoSplitTask task1 = createTask(startId, middleId);
            ForkJoinTask<Void> fork1 = task1.fork();
            DaoSplitTask task2 = createTask(middleId, endId);
            ForkJoinTask<Void> fork2 = task2.fork();
            fork1.join();
            fork2.join();
            insertCount.set(task1.getInsertCount());
            insertCount.addAndGet(task2.getInsertCount());
        } else {
            dbManager.execute(setting, () -> {
                insertCount.set(0);
                for (int iId = startId; iId < endId; iId++) {
                    execute(iId, insertCount);
                }
            });
        }
    }

    protected abstract DaoSplitTask createTask(int startId, int endId);

    protected abstract void execute(int iId, AtomicInteger insertCount);

    public int getInsertCount() {
        return insertCount.get();
    }
}
