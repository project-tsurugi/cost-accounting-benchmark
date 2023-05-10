package com.tsurugidb.benchmark.costaccounting.online.periodic;

import java.io.IOException;
import java.io.UncheckedIOException;
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

import com.tsurugidb.benchmark.costaccounting.batch.StringUtil;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 在庫履歴の追加
 */
public class BenchPeriodicUpdateStockTask extends BenchPeriodicTask {
    private static final Logger LOG = LoggerFactory.getLogger(BenchPeriodicUpdateStockTask.class);

    public static final String TASK_NAME = "update-stock";

    private TgTmSetting settingMain;
    private final int threadSize;

    public BenchPeriodicUpdateStockTask(int taskId) {
        super(TASK_NAME, taskId);
        this.threadSize = BenchConst.periodicSplitSize(TASK_NAME);
        LOG.info("split.size={}", threadSize);
    }

    @Override
    public void initializeSetting(OnlineConfig config) {
        this.settingMain = config.getSetting(LOG, this, () -> TgTxOption.ofLTX(StockHistoryDao.TABLE_NAME));
        setTxOptionDescription(settingMain);
    }

    @Override
    protected boolean execute1() {
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

        var exceptionList = new ArrayList<Exception>();
        for (var future : resultList) {
            try {
                future.get();
            } catch (Exception e) {
                exceptionList.add(e);
            }
        }
        if (!exceptionList.isEmpty()) {
            RuntimeException re;
            String message = "update-stock future.get() error";
            boolean isIoException = exceptionList.stream().anyMatch(e -> findIOException(e) != null);
            if (isIoException) {
                re = new UncheckedIOException(message, new IOException(message));
            } else {
                re = new RuntimeException(message);
            }
            for (var e : exceptionList) {
                re.addSuppressed(e);
            }
            throw re;
        }

        return true;
    }

    private static IOException findIOException(Exception e) {
        for (Throwable t = e; t != null; t = t.getCause()) {
            if (t instanceof IOException) {
                return (IOException) t;
            }
        }
        return null;
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
        var task = new BenchPeriodicUpdateStockTask(0);

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            List<Integer> factoryList = List.of();
            if (0 < args.length) {
                factoryList = StringUtil.toIntegerList(args[0]);
            }
            if (factoryList.isEmpty()) {
                factoryList = manager.execute(TgTmSetting.of(TgTxOption.ofRTX()), () -> {
                    return manager.getFactoryMasterDao().selectAllId();
                });
            }
            LOG.info("factoryList={}", StringUtil.toString(factoryList));

            task.initialize(factoryList, InitialData.DEFAULT_BATCH_DATE);

            LOG.info("start");
            long start = System.currentTimeMillis();
            task.execute();
            long end = System.currentTimeMillis();
            LOG.info("end {}", end - start);
        }
    }
}
