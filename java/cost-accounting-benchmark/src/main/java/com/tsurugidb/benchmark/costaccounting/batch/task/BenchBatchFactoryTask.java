package com.tsurugidb.benchmark.costaccounting.batch.task;

import java.time.LocalDate;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.init.BenchRandom;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

// 1 thread
public class BenchBatchFactoryTask implements Runnable, Callable<Void> {

    private static final TgTmSetting TX_BATCH = TgTmSetting.of( //
            TgTxOption.ofLTX(ResultTableDao.TABLE_NAME));

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

        dbManager.execute(TX_BATCH, () -> {
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
            dbManager.commit();
            commitCount += count;
            System.out.printf("commit (%s, %d), count=%d\n", batchDate, factoryId, count);
        } else {
            dbManager.rollback();
            rollbackCount += count;
            System.out.printf("rollback (%s, %d), count=%d\n", batchDate, factoryId, count);
        }
    }

    public final int getCommitCount() {
        return commitCount;
    }

    public final int getRollbackCount() {
        return rollbackCount;
    }
}
