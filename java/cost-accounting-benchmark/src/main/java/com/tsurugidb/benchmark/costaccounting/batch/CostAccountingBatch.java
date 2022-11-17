package com.tsurugidb.benchmark.costaccounting.batch;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchFactoryTask;
import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchItemTask;
import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchTxOption;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public class CostAccountingBatch {
    private static final Logger LOG = LoggerFactory.getLogger(CostAccountingBatch.class);

    private static final TgTmSetting TX_BATCH = TgTmSetting.of( //
            TgTxOption.ofLTX(ResultTableDao.TABLE_NAME));

    public static void main(String[] args) {
        LocalDate batchDate = InitialData.DEFAULT_BATCH_DATE;
        if (args.length >= 1) {
            batchDate = LocalDate.parse(args[0]);
        }

        List<Integer> factoryList = null;
        if (args.length >= 2) {
            if (!args[1].trim().equalsIgnoreCase("all")) {
                factoryList = StringUtil.toIntegerList(args[1]);
            }
        }

        int commitRatio = 100;
        if (args.length >= 3) {
            commitRatio = Integer.parseInt(args[2].trim());
        }

        String executeType = BenchConst.batchExecuteType();
        var config = new BatchConfig(executeType, batchDate, factoryList, commitRatio);
        config.setTxOptions(BenchConst.batchTsurugiTxOption());

        int exitCode = new CostAccountingBatch().main(config);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private BatchConfig config;
    private CostBenchDbManager dbManager;
    private final AtomicInteger commitCount = new AtomicInteger();
    private final AtomicInteger rollbackCount = new AtomicInteger();

    public CostAccountingBatch() {
    }

    public int main(BatchConfig config) {
        this.config = config;
        logStart();

        int exitCode;
        try (CostBenchDbManager manager = createDbManager()) {
            this.dbManager = manager;

            List<Integer> factoryList = config.getFactoryList();
            if (factoryList == null || factoryList.isEmpty()) {
                factoryList = getAllFactory();
                config.setFactoryList(factoryList);
            }

            LOG.info("batchDate={}", config.getBatchDate());
            LOG.info("factory={}", StringUtil.toString(config.getFactoryList()));
            LOG.info("commitRatio={}", config.getCommitRatio());

            String type = config.getExecuteType();
            LOG.info("batch.execute.type={}", type);
            switch (type) {
            case "sequential-single-tx":
                exitCode = executeSequentialSingleTx();
                break;
            case "sequential-factory-tx":
                exitCode = executeSequentialFactoryTx();
                break;
            case "parallel-single-tx":
                exitCode = executeParallelSingleTx();
                break;
            case "parallel-factory-tx":
                exitCode = executeParallelFactoryTx();
                break;
            case "stream":
                exitCode = executeStream();
                break;
            case "queue":
                exitCode = executeQueue();
                break;
            case "debug":
                exitCode = executeForDebug1();
                break;
            default:
                throw new UnsupportedOperationException(type);
            }
        }

        logEnd();
        return exitCode;
    }

    private CostBenchDbManager createDbManager() {
        int type = BenchConst.batchDbManagerType();
        return CostBenchDbManager.createInstance(type);
    }

    private LocalDateTime startTime;

    protected void logStart() {
        startTime = LocalDateTime.now();
        LOG.info("start {}", startTime);
    }

    protected void logEnd() {
        LOG.info("commit={}, rollback={}", commitCount.get(), rollbackCount.get());
        LocalDateTime endTime = LocalDateTime.now();
        LOG.info("end {}[ms]", startTime.until(endTime, ChronoUnit.MILLIS));
    }

    private List<Integer> getAllFactory() {
        FactoryMasterDao dao = dbManager.getFactoryMasterDao();

        var setting = TgTmSetting.of(TgTxOption.ofOCC(), TgTxOption.ofRTX());
        return dbManager.execute(setting, dao::selectAllId);
    }

    private int executeSequentialSingleTx() {
        var batchDate = config.getBatchDate();
        var factoryList = config.getFactoryList();

        BenchBatchItemTask itemTask = newBenchBatchItemTask(batchDate);

        var option = BenchBatchTxOption.of(config);
        LOG.info("tx={}", option);
        TgTmSetting setting = TgTmSetting.of(option);

        dbManager.execute(setting, () -> {
            int count = 0;
            for (int factoryId : factoryList) {
                BenchBatchFactoryTask thread = newBenchBatchFactoryThread(batchDate, factoryId);

                int c = thread.runInTransaction(itemTask);
                LOG.info("processed ({}, {}), count={}", batchDate, factoryId, c);
                count += c;
            }

            commitOrRollback(batchDate, count);
        });
        return 0;
    }

    private void commitOrRollback(LocalDate batchDate, int count) {
        int commitRatio = config.getCommitRatio();

        var random = new BenchRandom();
        int n = random.random(0, 99);
        if (n < commitRatio) {
            dbManager.commit(() -> {
                commitCount.addAndGet(count);
                LOG.info("commit ({}), count={}", batchDate, count);
            });
        } else {
            dbManager.rollback(() -> {
                rollbackCount.addAndGet(count);
                LOG.info("rollback ({}), count={}", batchDate, count);
            });
        }
    }

    private int executeSequentialFactoryTx() {
        var batchDate = config.getBatchDate();
        var factoryList = config.getFactoryList();

        for (int factoryId : factoryList) {
            BenchBatchFactoryTask thread = newBenchBatchFactoryThread(batchDate, factoryId);

            thread.run();
            addCount(thread);
        }
        return 0;
    }

    private int executeParallelSingleTx() {
        var batchDate = config.getBatchDate();
        var factoryList = config.getFactoryList();

        var count = new AtomicInteger(0);
        var threadList = factoryList.stream().map(factoryId -> {
            var thread = newBenchBatchFactoryThread(batchDate, factoryId);
            return new Callable<Void>() {
                private final BenchBatchItemTask itemTask = newBenchBatchItemTask(batchDate);

                @Override
                public Void call() throws Exception {
                    int c = thread.runInTransaction(itemTask);
                    LOG.info("processed ({}, {}), count={}", batchDate, factoryId, c);
                    count.addAndGet(c);
                    return null;
                }
            };
        }).collect(Collectors.toList());

        var option = BenchBatchTxOption.of(config);
        LOG.info("tx={}", option);
        TgTmSetting setting = TgTmSetting.of(option);

        dbManager.setSingleTransaction(true);
        dbManager.execute(setting, () -> {
            executeParallel(threadList);

            commitOrRollback(batchDate, count.get());
        });
        return 0;
    }

    private int executeParallelFactoryTx() {
        var batchDate = config.getBatchDate();
        var factoryList = config.getFactoryList();

        List<BenchBatchFactoryTask> threadList = factoryList.stream().map(factoryId -> newBenchBatchFactoryThread(batchDate, factoryId)).collect(Collectors.toList());

        int exitCode = executeParallel(threadList);

        for (BenchBatchFactoryTask thread : threadList) {
            addCount(thread);
        }

        return exitCode;
    }

    private int executeParallel(List<? extends Callable<Void>> threadList) {
        int exitCode = 0;

        int batchParallelism = BenchConst.batchParallelism();
        if (batchParallelism <= 0) {
            var factoryList = config.getFactoryList();
            batchParallelism = factoryList.size();
        }

        ExecutorService pool = Executors.newFixedThreadPool(batchParallelism);
        List<Future<Void>> resultList = Collections.emptyList();
        try {
            resultList = pool.invokeAll(threadList);
        } catch (InterruptedException e) {
            LOG.debug("InterruptedException", e);
            exitCode = 1;
        } finally {
            pool.shutdownNow();
        }

        for (Future<Void> result : resultList) {
            try {
                result.get(); // 例外が発生していた場合にそれを取り出す
            } catch (Exception e) {
                LOG.error("future exception", e);
                exitCode |= 2;
            }
        }
        return exitCode;
    }

    protected BenchBatchFactoryTask newBenchBatchFactoryThread(LocalDate batchDate, int factoryId) {
        BenchBatchFactoryTask task = new BenchBatchFactoryTask(config, dbManager, factoryId);
        return task;
    }

    private int executeStream() {
        var batchDate = config.getBatchDate();
        var factoryList = config.getFactoryList();

        dbManager.execute(TX_BATCH, () -> {
            ResultTableDao resultTableDao = dbManager.getResultTableDao();
            resultTableDao.deleteByFactories(factoryList, batchDate);

            BenchBatchFactoryTask thread = newBenchBatchFactoryThread(batchDate, -1);
            BenchBatchItemTask itemTask = newBenchBatchItemTask(batchDate);

            ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
            int[] count = { 0 };
            try (Stream<ItemManufacturingMaster> stream = itemManufacturingMasterDao.selectByFactories(factoryList, batchDate)) {
                stream.forEach(manufact -> {
                    count[0]++;
                    itemTask.execute(manufact);
                    if (count[0] % 10 == 0) {
                        LOG.info("executeStream progress {}", count[0]);
                    }
                });
            }

            thread.commitOrRollback(count[0]);
            addCount(thread);
        });
        return 0;
    }

    private int executeQueue() {
        var batchDate = config.getBatchDate();
        var factoryList = config.getFactoryList();

        ResultTableDao resultTableDao = dbManager.getResultTableDao();

        ConcurrentLinkedDeque<ItemManufacturingMaster> queue = dbManager.execute(TX_BATCH, () -> {
            ConcurrentLinkedDeque<ItemManufacturingMaster> q;
            resultTableDao.deleteByFactories(factoryList, batchDate);

            ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
            try (Stream<ItemManufacturingMaster> stream = itemManufacturingMasterDao.selectByFactories(factoryList, batchDate)) {
                q = stream.collect(Collectors.toCollection(ConcurrentLinkedDeque::new));
            }
            return q;
        });

        int size = 8;
        ExecutorService pool = Executors.newFixedThreadPool(size);
        List<Callable<Void>> list = Stream.generate(() -> new Callable<Void>() {

            @Override
            public Void call() throws Exception {
                BenchBatchFactoryTask thread = newBenchBatchFactoryThread(batchDate, -1);
                BenchBatchItemTask itemTask = newBenchBatchItemTask(batchDate);

                dbManager.execute(TX_BATCH, () -> {
                    int[] count = { 0 };
                    for (;;) {
                        ItemManufacturingMaster manufact = queue.pollLast();
                        if (manufact == null) {
                            break;
                        }
                        count[0]++;
                        itemTask.execute(manufact);
                    }

                    thread.commitOrRollback(count[0]);
                    addCount(thread);
                });
                return null;
            }
        }).limit(size).collect(Collectors.toList());

        int exitCode = 0;
        try {
            pool.invokeAll(list);
        } catch (InterruptedException e) {
            LOG.debug("InterruptedException", e);
            exitCode = 1;
        } finally {
            pool.shutdown();
        }
        return exitCode;
    }

    // TODO delete executeForDebug1()
    private int executeForDebug1() {
        var batchDate = config.getBatchDate();
        int factoryId = 1;
        int productId = 345859;

        ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
        ResultTableDao resultTableDao = dbManager.getResultTableDao();

        BenchBatchItemTask itemTask = newBenchBatchItemTask(batchDate);

        dbManager.execute(TX_BATCH, () -> {
            ItemManufacturingMaster manufact = itemManufacturingMasterDao.selectById(factoryId, productId, batchDate);

            resultTableDao.deleteByProductId(factoryId, batchDate, productId);

            itemTask.execute(manufact);
        });
        return 0;
    }

    protected BenchBatchItemTask newBenchBatchItemTask(LocalDate batchDate) {
        return new BenchBatchItemTask(dbManager, batchDate);
    }

    protected void addCount(BenchBatchFactoryTask thread) {
        commitCount.addAndGet(thread.getCommitCount());
        rollbackCount.addAndGet(thread.getRollbackCount());
    }
}
