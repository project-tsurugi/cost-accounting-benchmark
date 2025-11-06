/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.online.periodic;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.batch.StringUtil;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistoryDateTime;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.InsertSelectType;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.SqlDistinct;
import com.tsurugidb.benchmark.costaccounting.util.ThreadUtil;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 在庫履歴の追加
 */
public class BenchPeriodicUpdateStockTask extends BenchPeriodicTask {
    private static final Logger LOG = LoggerFactory.getLogger(BenchPeriodicUpdateStockTask.class);

    public static final String TASK_NAME = "update-stock";
    private static final SqlDistinct SQL_DISTINCT = BenchConst.sqlDistinct();
    private static final InsertSelectType INSERT_SELECT_TYPE = BenchConst.periodicInsertSelectType(TASK_NAME);

    private TgTmSetting settingPre;
    private TgTmSetting settingMain;
    private final int threadSize;
    private final ExecutorService service;
    private final int keepSize;
    private final boolean delete1;

    public BenchPeriodicUpdateStockTask(int taskId) {
        super(TASK_NAME, taskId);
        this.threadSize = BenchConst.periodicSplitSize(TASK_NAME);
        LOG.info("split.size={}", threadSize);
        if (threadSize <= 1) {
            this.service = null;
        } else {
            String threadName = String.format("%s.%d.thread-", TASK_NAME, taskId);
            this.service = ThreadUtil.newFixedThreadPool(threadName, threadSize);
        }
        LOG.info("insert_select_type={}", INSERT_SELECT_TYPE);
        this.keepSize = BenchConst.periodicKeepSize(TASK_NAME);
        LOG.info("keep.size={}", keepSize);
        this.delete1 = BenchConst.periodicDelete1(TASK_NAME);
        if (delete1) {
            LOG.info("delete1={}", delete1);
        }
    }

    BenchPeriodicUpdateStockTask(int threadSize, int keepSize) { // for test
        super(TASK_NAME, 0);
        this.threadSize = threadSize;
        this.service = null;
        this.keepSize = keepSize;
        this.delete1 = false;
    }

    @Override
    public void initializeSetting() {
        this.settingPre = TgTmSetting.ofAlways(TgTxOption.ofLTX() // RTXだと古いデータが読まれてしまうことがある
                .addInclusiveReadArea(StockHistoryDao.TABLE_NAME).label(TASK_NAME + ".pre"));
//      this.settingPre = TgTmSetting.ofAlways(TgTxOption.ofOCC().label(TASK_NAME + ".pre"));
        this.settingMain = config.getSetting(LOG, this, () -> {
            if (BenchConst.useReadArea()) {
                return TgTxOption.ofLTX(BenchConst.DEFAULT_TX_OPTION).addWritePreserve(StockHistoryDao.TABLE_NAME) //
                        .addInclusiveReadArea(CostMasterDao.TABLE_NAME);
            } else {
                return TgTxOption.ofLTX(StockHistoryDao.TABLE_NAME);
            }
        });
        setTxOptionDescription(settingMain);
    }

    @Override
    protected boolean execute1() {
        long start = System.currentTimeMillis();
        var deleteDateTime = getDeleteDateTime();
        LOG.info("deleteDateTime={} {}[ms]", deleteDateTime, System.currentTimeMillis() - start);
        start = System.currentTimeMillis();
        try {
            if (threadSize <= 1) {
                return executeAll(deleteDateTime);
            } else {
                return executeFactory(threadSize, deleteDateTime);
            }
        } finally {
            if (deleteDateTime == null) {
                LOG.info("insert {}[ms]", System.currentTimeMillis() - start);
            } else {
                LOG.info("delete+insert {}[ms]", System.currentTimeMillis() - start);
            }
        }
    }

    private StockHistoryDateTime getDeleteDateTime() {
        if (this.keepSize <= 0) {
            return null;
        }

        var list = dbManager.execute(settingPre, () -> {
            var dao = dbManager.getStockHistoryDao();
            switch (SQL_DISTINCT) {
            case GROUP:
                return dao.selectGroupByDateTime();
            case DISTINCT:
                return dao.selectDistinctDateTime();
            default:
                throw new AssertionError(SQL_DISTINCT);
            }
        });
        return getDeleteDateTime(list);
    }

    StockHistoryDateTime getDeleteDateTime(List<StockHistoryDateTime> list) {
        int i = list.size() - this.keepSize;
        if (i < 0) {
            return null;
        }

        if (this.delete1) {
            return list.get(0); // 最も古い履歴
        }

        return list.get(i);
    }

    private boolean executeAll(StockHistoryDateTime deleteDateTime) {
        return dbManager.execute(settingMain, () -> {
            deleteAllInTransaction(deleteDateTime);
            executeAllInTransaction();

            return true;
        });
    }

    private void deleteAllInTransaction(StockHistoryDateTime deleteDateTime) {
        if (deleteDateTime == null) {
            return;
        }

        var dao = dbManager.getStockHistoryDao();
        if (this.delete1) {
            dao.deleteByDateTime(deleteDateTime.getSDate(), deleteDateTime.getSTime());
        } else {
            dao.deleteOldDateTime(deleteDateTime.getSDate(), deleteDateTime.getSTime());
        }
    }

    private void executeAllInTransaction() {
        var now = LocalDateTime.now();

        if (INSERT_SELECT_TYPE == InsertSelectType.SELECT_AND_INSERT) {
            var costMasterDao = dbManager.getCostMasterDao();
            try (var stream = costMasterDao.selectAll()) {
                streamInsert(stream, now);
            }
            return;
        }

        var dao = dbManager.getStockHistoryDao();
        dao.insertSelectFromCostMaster(now.toLocalDate(), now.toLocalTime());
    }

    private void streamInsert(Stream<CostMaster> stream, LocalDateTime now) {
        // var count = new AtomicInteger(0);
        var dao = dbManager.getStockHistoryDao();
        final int batchSize = 10000;
        var list = new ArrayList<StockHistory>(batchSize);
        stream.forEach(cost -> {
            var entity = new StockHistory();
            entity.setSDate(now.toLocalDate());
            entity.setSTime(now.toLocalTime());
            entity.setSFId(cost.getCFId());
            entity.setSIId(cost.getCIId());
            entity.setSStockUnit(cost.getCStockUnit());
            entity.setSStockQuantity(cost.getCStockQuantity());
            entity.setSStockAmount(cost.getCStockAmount());
            list.add(entity);
            if (list.size() >= batchSize) {
                dao.insertBatch(list);
//              count.addAndGet(list.size());
//              LOG.info("streamInsert() inserted {}", count.get());
                list.clear();
            }
        });
        if (!list.isEmpty()) {
            dao.insertBatch(list);
//          count.addAndGet(list.size());
//          LOG.info("streamInsert() inserted {}", count.get());
        }
    }

    private boolean executeFactory(int threadSize, StockHistoryDateTime deleteDateTime) {
        var now = LocalDateTime.now();

        List<Callable<Void>> taskList = new ArrayList<>(factoryList.size());
        for (int factoryId : factoryList) {
            var task = new FactoryTask(factoryId, now, deleteDateTime);
            taskList.add(task);
        }

        List<Future<Void>> resultList = Collections.emptyList();
        try {
            resultList = service.invokeAll(taskList);
        } catch (InterruptedException e) {
            LOG.debug("InterruptedException", e);
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
        private final LocalDateTime now;
        private final StockHistoryDateTime deleteDateTime;

        public FactoryTask(int factoryId, LocalDateTime now, StockHistoryDateTime deleteDateTime) {
            this.factoryId = factoryId;
            this.now = now;
            this.deleteDateTime = deleteDateTime;
        }

        @Override
        public Void call() throws Exception {
            try {
                dbManager.execute(settingMain, () -> {
                    deleteInTransaction();
                    executeInTransaction();
                });
            } catch (Throwable e) {
                LOG.error("update-stock factory{} error", factoryId, e);
                throw e;
            }
            return null;
        }

        private void deleteInTransaction() {
            if (deleteDateTime == null) {
                return;
            }

            var dao = dbManager.getStockHistoryDao();
            if (delete1) {
                dao.deleteByDateTime(deleteDateTime.getSDate(), deleteDateTime.getSTime(), factoryId);
            } else {
                dao.deleteOldDateTime(deleteDateTime.getSDate(), deleteDateTime.getSTime(), factoryId);
            }
        }

        private void executeInTransaction() {
            if (INSERT_SELECT_TYPE == InsertSelectType.SELECT_AND_INSERT) {
                var costMasterDao = dbManager.getCostMasterDao();
                try (var stream = costMasterDao.selectByFactory(factoryId)) {
                    streamInsert(stream, now);
                }
                return;
            }

            var dao = dbManager.getStockHistoryDao();
            dao.insertSelectFromCostMaster(now.toLocalDate(), now.toLocalTime(), factoryId);
        }
    }

    @Override
    public void close() {
        if (this.service != null) {
            service.shutdownNow();
        }
    }

    // for test
    public static void main(String... args) {
        try (var task = new BenchPeriodicUpdateStockTask(0); //
                CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            var config = new OnlineConfig(InitialData.DEFAULT_BATCH_DATE);
            config.setTxOption(TASK_NAME, "LTX");
            task.setDao(config, manager);
            task.initializeSetting();

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

            task.initialize(factoryList, config.getBatchDate());

            LOG.info("start");
            long start = System.currentTimeMillis();
            task.execute();
            long end = System.currentTimeMillis();
            LOG.info("end {}", end - start);
        }
    }
}
