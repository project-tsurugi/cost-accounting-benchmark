package com.tsurugidb.benchmark.costaccounting.batch.task;

import java.time.LocalDate;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

// 1 thread
public class BenchBatchFactoryTask implements Runnable, Callable<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(BenchBatchFactoryTask.class);

    private final CostBenchDbManager dbManager;
    private final int commitRatio;
    private final LocalDate batchDate;
    private final int factoryId;

    private final BenchRandom random = new BenchRandom();

    private int commitCount = 0;
    private int rollbackCount = 0;

    public BenchBatchFactoryTask(CostBenchDbManager dbManager, int commitRatio, LocalDate batchDate, int factoryId) {
        this.dbManager = dbManager;
        this.commitRatio = commitRatio;
        this.batchDate = batchDate;
        this.factoryId = factoryId;
    }

    @Override
    public void run() {
        BenchBatchItemTask itemTask = new BenchBatchItemTask(dbManager, batchDate);

        var option = BenchBatchTxOption.of(factoryId);
        LOG.info("tx={}", option);
        TgTmSetting setting = TgTmSetting.of(option);

        dbManager.execute(setting, () -> {
            deleteResult();

            int[] count = { 0 };
            try (Stream<ItemManufacturingMaster> stream = selectManufacturingItem()) {
                stream.forEach(item -> {
                    count[0]++;
                    itemTask.execute(item);
                });
            }

            commitOrRollback(count[0]);
        });
    }

    private void deleteResult() {
        ResultTableDao dao = dbManager.getResultTableDao();
        dao.deleteByFactory(factoryId, batchDate);
    }

    private Stream<ItemManufacturingMaster> selectManufacturingItem() {
        ItemManufacturingMasterDao dao = dbManager.getItemManufacturingMasterDao();

        return dao.selectByFactory(factoryId, batchDate);
    }

    @Override
    public final Void call() {
        run();
        return null;
    }

    public void commitOrRollback(int count) {
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

    public final int getCommitCount() {
        return commitCount;
    }

    public final int getRollbackCount() {
        return rollbackCount;
    }
}
