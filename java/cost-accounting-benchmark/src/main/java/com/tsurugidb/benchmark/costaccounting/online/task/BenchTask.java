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

    protected CostBenchDbManager dbManager;

    public BenchTask(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public abstract void initializeSetting(OnlineConfig config);

    public void setDao(CostBenchDbManager dbManager) {
        this.dbManager = dbManager;
    }

    protected final void incrementStartCounter() {
        dbManager.incrementTaskCounter(title, CounterName.TASK_START);
    }

    protected final void incrementNothingCounter() {
        dbManager.incrementTaskCounter(title, CounterName.TASK_NOTHING);
    }

    protected final void incrementSuccessCounter() {
        dbManager.incrementTaskCounter(title, CounterName.TASK_SUCCESS);
    }

    protected final void incrementFailCounter() {
        dbManager.incrementTaskCounter(title, CounterName.TASK_FAIL);
    }

    protected static final CostBenchDbManager createCostBenchDbManagerForTest() {
        var config = CostAccountingOnline.createDefaultConfig(InitialData.DEFAULT_BATCH_DATE);
        return CostAccountingOnline.createDbManager(config);
    }
}
