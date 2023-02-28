package com.tsurugidb.benchmark.costaccounting.online;

import java.text.MessageFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.online.random.CostAccountingOnlineAppRandom;
import com.tsurugidb.benchmark.costaccounting.online.schedule.CostAccountingOnlineAppSchedule;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineNewItemTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineShowCostTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineShowQuantityTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineShowWeightTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateCostTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateManufacturingTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateMaterialTask;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.option.TsurugiDefaultRetryPredicate;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

public class CostAccountingOnline {
    private static final Logger LOG = LoggerFactory.getLogger(CostAccountingOnline.class);

    public static void main(String[] args) throws Exception {
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

        int exitCode = new CostAccountingOnline(batchDate).main();
        if (exitCode != 0) {
            System.exit(exitCode);
        }
    }

    private final LocalDate batchDate;
    private CostBenchDbManager dbManager;
    private final AtomicBoolean terminationRequested = new AtomicBoolean(false);
    private final AtomicBoolean wait = new AtomicBoolean(true);
    private final AtomicBoolean done = new AtomicBoolean(false);
    private ExExecutorService service;

    public CostAccountingOnline(LocalDate batchDate) {
        this.batchDate = batchDate;
    }

    private int main() throws Exception {
        try (CostBenchDbManager manager = createDbManager()) {
            this.dbManager = manager;

            var appList = createOnlineApp();
            CostBenchDbManager.initCounter();
            try {
                startShutdownHook();
                return executeOnlineApp(appList);
            } finally {
                done.set(true);
            }
        }
    }

    public static CostBenchDbManager createDbManager() {
        TsurugiDefaultRetryPredicate.setInstance(new TsurugiDefaultRetryPredicate() {
            @Override
            protected boolean testOcc(TsurugiTransaction transaction, TsurugiDiagnosticCodeProvider e) {
                if (super.testOcc(transaction, e)) {
                    return true;
                }

                var lowCode = e.getDiagnosticCode();
                if (lowCode == SqlServiceCode.ERR_ABORTED) {
                    return true;
                }
                return false;
            }
        });

        var type = BenchConst.onlineDbManagerType();
        var isolationLevel = BenchConst.onlineJdbcIsolationLevel();
        boolean isMultiSession = BenchConst.onlineDbManagerMultiSession();
        if (BenchConst.dbmsType() != DbmsType.TSURUGI) {
            LOG.info("online.jdbc.isolation.level={}", isolationLevel);
        }
        return CostBenchDbManager.createInstance(type, isolationLevel, isMultiSession);
    }

    private List<? extends Runnable> createOnlineApp() {
        BenchOnlineTask.clearOnceLog();

        String type = BenchConst.onlineType();
        switch (type.toLowerCase()) {
        case BenchConst.ONLINE_RANDOM:
            return createOnlineAppRandom();
        case BenchConst.ONLINE_SCHEDULE:
            return createOnlineAppSchedule();
        default:
            throw new IllegalArgumentException(MessageFormat.format("invalid online.type ({0})", type));
        }
    }

    private List<CostAccountingOnlineAppRandom> createOnlineAppRandom() {
        int threadSize = BenchConst.onlineThreadSize();
        LOG.info("create random app. threadSize={}", threadSize);
        var appList = new ArrayList<CostAccountingOnlineAppRandom>(threadSize);

        List<Integer> factoryList = getAllFactory();
        for (int i = 0; i < threadSize; i++) {
            var app = new CostAccountingOnlineAppRandom(i, dbManager, factoryList, batchDate, terminationRequested);
            appList.add(app);
        }

        return appList;
    }

    private List<CostAccountingOnlineAppSchedule> createOnlineAppSchedule() {
        var factoryList = getAllFactory();

        var taskList = new ArrayList<Supplier<BenchOnlineTask>>();
        taskList.add(BenchOnlineNewItemTask::new);
        taskList.add(BenchOnlineUpdateManufacturingTask::new);
        taskList.add(BenchOnlineUpdateMaterialTask::new);
        taskList.add(BenchOnlineUpdateCostTask::new);
        taskList.add(BenchOnlineShowWeightTask::new);
        taskList.add(BenchOnlineShowQuantityTask::new);
        taskList.add(BenchOnlineShowCostTask::new);

        var appList = new ArrayList<CostAccountingOnlineAppSchedule>();
        for (var taskSupplier : taskList) {
            var task = taskSupplier.get();
            String taskName = task.getTitle();
            int size = BenchConst.onlineThreadSize(taskName);
            LOG.info("create schedule app. {}={}", taskName, size);

            for (int i = 0; i < size; i++, task = null) {
                if (task == null) {
                    task = taskSupplier.get();
                }
                task.setDao(dbManager);

                var app = new CostAccountingOnlineAppSchedule(task, i, factoryList, batchDate, terminationRequested);
                appList.add(app);
            }
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

    public void start() throws Exception {
        this.dbManager = createDbManager();

        var appList = createOnlineApp();
        this.service = newExecutorService(appList.size());
        // オンラインアプリを実行する
        appList.parallelStream().forEach(app -> service.submit(app));
    }

    public int terminate() {
        try (var c = dbManager) {
            terminateOnlineApp();
            terminateService();
        }
        return exceptionList.size();
    }
}
