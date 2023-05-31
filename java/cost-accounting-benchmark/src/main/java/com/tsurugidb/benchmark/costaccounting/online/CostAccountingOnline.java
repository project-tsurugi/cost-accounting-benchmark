package com.tsurugidb.benchmark.costaccounting.online;

import java.text.MessageFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.IntFunction;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager.DbManagerPurpose;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.online.periodic.BenchPeriodicTask;
import com.tsurugidb.benchmark.costaccounting.online.periodic.BenchPeriodicUpdateStockTask;
import com.tsurugidb.benchmark.costaccounting.online.periodic.CostAccountingPeriodicAppSchedule;
import com.tsurugidb.benchmark.costaccounting.online.random.CostAccountingOnlineAppRandom;
import com.tsurugidb.benchmark.costaccounting.online.schedule.CostAccountingOnlineAppSchedule;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineNewItemTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineShowCostTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineShowQuantityTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineShowWeightTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateCostAddTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateCostSubTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateManufacturingTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateMaterialTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchTask;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.retry.TgTmRetryInstruction;
import com.tsurugidb.iceaxe.transaction.manager.retry.TsurugiDefaultRetryPredicate;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

public class CostAccountingOnline {
    private static final Logger LOG = LoggerFactory.getLogger(CostAccountingOnline.class);

    public static void main(String... args) throws Exception {
        int exitCode = main0(args);
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    public static int main0(String... args) throws Exception {
        LocalDate batchDate;
        if (args.length > 0) {
            try {
                batchDate = LocalDate.parse(args[0]);
            } catch (Exception e) {
                throw new IllegalArgumentException("invalid batchDate (args[0])", e);
            }
        } else {
            batchDate = BenchConst.initBatchDate();
        }

        return new CostAccountingOnline().main(batchDate);
    }

    private CostBenchDbManager dbManager;
    private final AtomicBoolean terminationRequested = new AtomicBoolean(false);
    private final AtomicBoolean wait = new AtomicBoolean(true);
    private final AtomicBoolean done = new AtomicBoolean(false);
    private ExExecutorService service;

    private int main(LocalDate batchDate) throws Exception {
        var config = createDefaultConfig(batchDate);
        try (CostBenchDbManager manager = createDbManager(config)) {
            this.dbManager = manager;

            var appList = createOnlineApp(config);
            CostBenchDbManager.initCounter();
            try {
                startShutdownHook();
                return executeOnlineApp(appList);
            } finally {
                done.set(true);
            }
        }
    }

    public static OnlineConfig createDefaultConfig(LocalDate batchDate) {
        return createDefaultConfig(batchDate, true);
    }

    public static OnlineConfig createDefaultConfig(LocalDate batchDate, boolean txOption) {
        var config = new OnlineConfig(batchDate);
        config.setIsolationLevel(BenchConst.onlineJdbcIsolationLevel());
        config.setMultiSession(BenchConst.onlineDbManagerMultiSession());
        config.setCoverRate(BenchConst.onlineCoverRate());

        for (String taskName : BenchOnlineTask.TASK_NAME_LIST) {
            if (txOption) {
                config.setTxOption(taskName, BenchConst.onlineTsurugiTxOption(taskName));
            }
            config.setThreadSize(taskName, BenchConst.onlineThreadSize(taskName));
        }
        for (String taskName : BenchPeriodicTask.TASK_NAME_LIST) {
            if (txOption) {
                config.setTxOption(taskName, BenchConst.periodicTsurugiTxOption(taskName));
            }
            config.setThreadSize(taskName, BenchConst.periodicThreadSize(taskName));
        }

        return config;
    }

    public static CostBenchDbManager createDbManager(OnlineConfig config) {
        TsurugiDefaultRetryPredicate.setInstance(new TsurugiDefaultRetryPredicate() {
            @Override
            protected TgTmRetryInstruction testOcc(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
                var code = e.getDiagnosticCode();
                if (code == SqlServiceCode.ERR_ABORTED) {
                    return TgTmRetryInstruction.ofRetryable(code);
                }

                return super.testOcc(transaction, e);
            }
        });

        var type = BenchConst.onlineDbManagerType();
        var isolationLevel = config.getIsolationLevel();
        boolean isMultiSession = config.isMultiSession();
        if (BenchConst.dbmsType() != DbmsType.TSURUGI) {
            LOG.info("online.jdbc.isolation.level={}", isolationLevel);
        }
        return CostBenchDbManager.createInstance(type, DbManagerPurpose.ONLINE, isolationLevel, isMultiSession);
    }

    private List<? extends Runnable> createOnlineApp(OnlineConfig config) {
        String type = BenchConst.onlineType();
        switch (type.toLowerCase()) {
        case BenchConst.ONLINE_RANDOM:
            return createOnlineAppRandom(config);
        case BenchConst.ONLINE_SCHEDULE:
            return createOnlineAppSchedule(config);
        default:
            throw new IllegalArgumentException(MessageFormat.format("invalid online.type ({0})", type));
        }
    }

    private List<CostAccountingOnlineAppRandom> createOnlineAppRandom(OnlineConfig config) {
        int threadSize = BenchConst.onlineRandomThreadSize();
        LOG.info("create random app. threadSize={}", threadSize);
        var appList = new ArrayList<CostAccountingOnlineAppRandom>(threadSize);

        List<Integer> factoryList = getAllFactory();
        for (int i = 0; i < threadSize; i++) {
            var app = new CostAccountingOnlineAppRandom(config, i, dbManager, factoryList, terminationRequested);
            appList.add(app);
        }

        return appList;
    }

    private List<Runnable> createOnlineAppSchedule(OnlineConfig config) {
        var factoryList = getAllFactory();

        var taskList = new ArrayList<IntFunction<BenchTask>>();
        taskList.add(BenchOnlineNewItemTask::new);
        taskList.add(BenchOnlineUpdateManufacturingTask::new);
        taskList.add(BenchOnlineUpdateMaterialTask::new);
        taskList.add(BenchOnlineUpdateCostAddTask::new);
        taskList.add(BenchOnlineUpdateCostSubTask::new);
        taskList.add(BenchOnlineShowWeightTask::new);
        taskList.add(BenchOnlineShowQuantityTask::new);
        taskList.add(BenchOnlineShowCostTask::new);
        taskList.add(BenchPeriodicUpdateStockTask::new);
        BenchTask.clearPrepareDataAll();

        var appList = new ArrayList<Runnable>();

        var prepareService = Executors.newCachedThreadPool();
        try {
            var prepareList = new ArrayList<Future<?>>();

            for (var taskSupplier : taskList) {
                var task = taskSupplier.apply(0);

                String taskName = task.getTitle();
                int size = config.getThreadSize(taskName);
                LOG.info("create schedule app. {}={}", taskName, size);

                for (int i = 0; i < size; i++, task = null) {
                    if (task == null) {
                        task = taskSupplier.apply(i);
                    }
                    task.setDao(dbManager);
                    task.initializeSetting(config);

                    var finalTask = task;
                    prepareList.add(prepareService.submit(() -> finalTask.executePrepare(config)));

                    Runnable app;
                    if (task instanceof BenchOnlineTask) {
                        var onlineTask = (BenchOnlineTask) task;
                        app = new CostAccountingOnlineAppSchedule(onlineTask, i, factoryList, config.getBatchDate(), terminationRequested);
                    } else {
                        var periodicTask = (BenchPeriodicTask) task;
                        app = new CostAccountingPeriodicAppSchedule(periodicTask, i, factoryList, config.getBatchDate(), terminationRequested);
                    }
                    appList.add(app);
                }
            }

            var exceptionList = new ArrayList<Exception>();
            for (var future : prepareList) {
                try {
                    future.get();
                } catch (Exception e) {
                    exceptionList.add(e);
                }
            }
            if (!exceptionList.isEmpty()) {
                var re = new RuntimeException("createOnlineAppSchedule future.get() error");
                for (var e : exceptionList) {
                    re.addSuppressed(e);
                }
                throw re;
            }

            dbManager.closeConnection();
        } finally {
            prepareService.shutdownNow();
        }

        return appList;
    }

    private List<Integer> getAllFactory() {
        FactoryMasterDao dao = dbManager.getFactoryMasterDao();

        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC().label("select all factory"));
        return dbManager.execute(setting, dao::selectAllId);
    }

    private void startShutdownHook() {
        wait.set(true);
        done.set(false);

        Runtime.getRuntime().addShutdownHook(new Thread() {
            @Override
            public void run() {
                LOG.debug("shutdown-hook start");
                wait.set(false);

                // 終了待ち
                while (!done.get()) {
                    try {
                        Thread.sleep(100);
                    } catch (InterruptedException ignore) {
                        // ignore
                    }
                }
            }
        });
    }

    private int executeOnlineApp(List<? extends Runnable> appList) throws Exception {
        this.service = newExecutorService(appList.size());
        try {
            // オンラインアプリを実行する
            appList.parallelStream().forEach(app -> service.submit(app));

            while (wait.get()) {
                Thread.sleep(100);
            }
        } finally {
            // オンラインアプリを終了する
            terminateOnlineApp();
            terminateService();
            LOG.info("Counter infos: \n---\n{}---", CostBenchDbManager.createCounterReport());
        }
        return exceptionList.size();
    }

    private ExExecutorService newExecutorService(int size) {
        exceptionList.clear();
        // return Executors.newFixedThreadPool(size);
        return new ExExecutorService(size);
    }

    private final List<Exception> exceptionList = new CopyOnWriteArrayList<>();

    private class ExExecutorService extends ThreadPoolExecutor {

        public ExExecutorService(int nThreads) {
            super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            Future<?> task = (Future<?>) r;
            try {
                task.get();
            } catch (InterruptedException | CancellationException ignore) {
                terminateOnlineApp();
            } catch (Exception e) {
                terminateOnlineApp();
                exceptionList.add(e);
            }
        }

        public void printException() {
            for (Exception e : exceptionList) {
                LOG.error("thread exception report", e);
            }
        }
    }

    private void terminateOnlineApp() {
        terminationRequested.set(true);
    }

    private void terminateService() {
        if (service != null) {
            try {
                service.shutdown();
                try {
                    service.awaitTermination(5, TimeUnit.MINUTES);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } finally {
                // 例外出力
                service.printException();
            }
        }
    }

    private List<Future<?>> futureList;

    public void start(OnlineConfig config) throws Exception {
        this.dbManager = createDbManager(config);

        var appList = createOnlineApp(config);
        this.service = newExecutorService(appList.size());
        // オンラインアプリを実行する
        this.futureList = appList.parallelStream().map(app -> service.submit(app)).collect(Collectors.toList());
    }

    public int terminate() {
        try (var c = dbManager) {
            terminateOnlineApp();
            if (futureList != null) {
                for (var future : futureList) {
                    try {
                        future.get(2, TimeUnit.HOURS);
                    } catch (Exception e) {
                        exceptionList.add(e);
                        LOG.warn("terminate future.get() error", e);
                    }
                }
            }
            terminateService();
        }
        return exceptionList.size();
    }
}
