package com.tsurugidb.benchmark.costaccounting.online.periodic;

import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 在庫履歴の追加
 */
public class BenchPeriodicUpdateStockTask extends BenchPeriodicTask {
    private static final Logger LOG = LoggerFactory.getLogger(BenchPeriodicUpdateStockTask.class);

    public static final String TASK_NAME = "update-stock";

    private final TgTmSetting settingMain;

    public BenchPeriodicUpdateStockTask() {
        super(TASK_NAME);
        this.settingMain = getSetting(() -> TgTxOption.ofLTX(StockHistoryDao.TABLE_NAME));
    }

    @Override
    protected String getTsurugiTxOption() {
        return BenchConst.periodicTsurugiTxOption(title);
    }

    @Override
    protected boolean execute1() {
        int threadSize = BenchConst.periodicSplitSize(TASK_NAME);
        if (threadSize <= 1) {
            return executeAll();
        } else {
            return executeFactory(threadSize);
        }
    }

    private boolean executeAll() {
        return dbManager.execute(settingMain, () -> {
            executeAllInTransaction();

            return true;
        });
    }

    private void executeAllInTransaction() {
        var time = LocalTime.now();

        if (!BenchConst.WORKAROUND) {
            // TODO select-insert
            throw new AssertionError("implemtens select-insert");
        }

        var costMasterDao = dbManager.getCostMasterDao();
        try (var stream = costMasterDao.selectAll()) {
            streamInsert(stream, time);
        }
    }

    private void streamInsert(Stream<CostMaster> stream, LocalTime time) {
        var dao = dbManager.getStockHistoryDao();
        stream.map(cost -> {
            var entity = new StockHistory();
            entity.setSDate(date);
            entity.setSFId(cost.getCFId());
            entity.setSIId(cost.getCIId());
            entity.setSTime(time);
            entity.setSStockUnit(cost.getCStockUnit());
            entity.setSStockQuantity(cost.getCStockQuantity());
            entity.setSStockAmount(cost.getCStockAmount());
            return entity;
        }).forEach(dao::insert);
    }

    private boolean executeFactory(int threadSize) {
        var time = LocalTime.now();

        List<Callable<Void>> taskList = new ArrayList<>(factoryList.size());
        for (int factoryId : factoryList) {
            var task = new FactoryTask(factoryId, time);
            taskList.add(task);
        }

        ExecutorService service = Executors.newFixedThreadPool(threadSize);
        List<Future<Void>> resultList = Collections.emptyList();
        try {
            resultList = service.invokeAll(taskList);
        } catch (InterruptedException e) {
            LOG.debug("InterruptedException", e);
        } finally {
            service.shutdownNow();
        }

        RuntimeException re = null;
        for (var future : resultList) {
            try {
                future.get();
            } catch (Exception e) {
                if (re == null) {
                    re = new RuntimeException("update-stock future error");
                }
                re.addSuppressed(e);
            }
        }
        if (re != null) {
            throw re;
        }

        return true;
    }

    private class FactoryTask implements Callable<Void> {
        private final int factoryId;
        private final LocalTime time;

        public FactoryTask(int factoryId, LocalTime time) {
            this.factoryId = factoryId;
            this.time = time;
        }

        @Override
        public Void call() throws Exception {
            try {
                dbManager.execute(settingMain, () -> {
                    executeInTransaction();
                });
            } catch (Throwable e) {
                LOG.error("update-stock factory{} error", factoryId, e);
                throw e;
            }
            return null;
        }

        private void executeInTransaction() {
            if (!BenchConst.WORKAROUND) {
                // TODO select-insert
                throw new AssertionError("implemtens select-insert");
            }

            var costMasterDao = dbManager.getCostMasterDao();
            try (var stream = costMasterDao.selectByFactory(factoryId)) {
                streamInsert(stream, time);
            }
        }
    }

    // for test
    public static void main(String... args) {
        var task = new BenchPeriodicUpdateStockTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(List.of(1), InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
