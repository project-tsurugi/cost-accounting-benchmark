package com.tsurugidb.benchmark.costaccounting.online.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;

public abstract class BenchTask {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    protected final String title;
    protected final int taskId;

    protected CostBenchDbManager dbManager;

    private long startTime;

    public BenchTask(String title, int taskId) {
        this.title = title;
        this.taskId = taskId;
    }

    public String getTitle() {
        return title;
    }

    public abstract void initializeSetting(OnlineConfig config);

    public void setDao(CostBenchDbManager dbManager) {
        this.dbManager = dbManager;
    }

    public void executePrepare(OnlineConfig config) {
        // do override
    }

    protected final void incrementStartCounter() {
        this.startTime = System.nanoTime();
        dbManager.incrementTaskCounter(title, CounterName.TASK_START);
    }

    protected final void incrementNothingCounter() {
        long endTime = System.nanoTime();
        dbManager.incrementTaskCounter(title, CounterName.TASK_NOTHING);
        dbManager.addTaskTime(title, CounterName.TASK_NOTHING, endTime - startTime);
    }

    protected final void incrementSuccessCounter() {
        long endTime = System.nanoTime();
        dbManager.incrementTaskCounter(title, CounterName.TASK_SUCCESS);
        dbManager.addTaskTime(title, CounterName.TASK_SUCCESS, endTime - startTime);
    }

    protected final void incrementFailCounter() {
        long endTime = System.nanoTime();
        dbManager.incrementTaskCounter(title, CounterName.TASK_FAIL);
        dbManager.addTaskTime(title, CounterName.TASK_FAIL, endTime - startTime);
    }

    protected static final CostBenchDbManager createCostBenchDbManagerForTest() {
        var config = CostAccountingOnline.createDefaultConfig(InitialData.DEFAULT_BATCH_DATE);
        return CostAccountingOnline.createDbManager(config);
    }
}
