package com.tsurugidb.benchmark.costaccounting.batch.task;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

// 1 thread
public class BenchBatchFactoryTask implements Runnable, Callable<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(BenchBatchFactoryTask.class);

    private final BatchConfig config;
    private final CostBenchDbManager dbManager;
    private final int factoryId;
    private final AtomicInteger tryCounter = new AtomicInteger(0);

    private final BenchRandom random = new BenchRandom();

    private int commitCount = 0;
    private int rollbackCount = 0;

    public BenchBatchFactoryTask(BatchConfig config, CostBenchDbManager dbManager, int factoryId) {
        this.config = config;
        this.dbManager = dbManager;
        this.factoryId = factoryId;
    }

    @Override
    public void run() {
        var batchDate = config.getBatchDate();
        BenchBatchItemTask itemTask = new BenchBatchItemTask(dbManager, batchDate);

        var option = BenchBatchTxOption.of(config, factoryId);
        LOG.info("tx={}", option);
        TgTmSetting setting = TgTmSetting.of(option);

        dbManager.execute(setting, () -> {
            tryCounter.incrementAndGet();
            int count = runInTransaction(itemTask);
            commitOrRollback(count);
        });
    }

    public int runInTransaction(BenchBatchItemTask itemTask) {
        deleteResult();

        int[] count = { 0 };
        try (Stream<ItemManufacturingMaster> stream = selectManufacturingItem()) {
            stream.forEach(item -> {
                count[0]++;
                itemTask.execute(item);
            });
        }

        return count[0];
    }

    private void deleteResult() {
        ResultTableDao dao = dbManager.getResultTableDao();

        var batchDate = config.getBatchDate();
        dao.deleteByFactory(factoryId, batchDate);
    }

    private Stream<ItemManufacturingMaster> selectManufacturingItem() {
        ItemManufacturingMasterDao dao = dbManager.getItemManufacturingMasterDao();

        var batchDate = config.getBatchDate();
        return dao.selectByFactory(factoryId, batchDate);
    }

    @Override
    public final Void call() {
        run();
        return null;
    }

    public void commitOrRollback(int count) {
        var batchDate = config.getBatchDate();
        int commitRatio = config.getCommitRatio();

        int n = random.random(0, 99);
        if (n < commitRatio) {
            dbManager.commit(() -> {
                commitCount += count;
                LOG.info("commit ({}, {}), count={}", batchDate, factoryId, count);
            });
        } else {
            dbManager.rollback(() -> {
                rollbackCount += count;
                LOG.info("rollback ({}, {}), count={}", batchDate, factoryId, count);
            });
        }
    }

    public final int getTryCount() {
        return tryCounter.get();
    }

    public final int getAbortCount() {
        return tryCounter.get() - 1;
    }

    public final int getCommitCount() {
        return commitCount;
    }

    public final int getRollbackCount() {
        return rollbackCount;
    }
}
