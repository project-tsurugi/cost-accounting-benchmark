package com.example.nedo.batch.task;

import java.time.LocalDate;
import java.util.concurrent.Callable;
import java.util.stream.Stream;

import com.example.nedo.init.BenchRandom;
import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

// 1 thread
public class BenchBatchFactoryTask implements Runnable, Callable<Void> {

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

        dbManager.execute(() -> {
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
