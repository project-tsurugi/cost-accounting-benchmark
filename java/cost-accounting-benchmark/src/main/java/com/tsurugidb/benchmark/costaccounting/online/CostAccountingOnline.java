package com.tsurugidb.benchmark.costaccounting.online;

import java.text.MessageFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.online.random.CostAccountingOnlineThread;
import com.tsurugidb.benchmark.costaccounting.online.schedule.BenchOnlineSchedule;
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

        String type = BenchConst.onlineType();
        try (CostBenchDbManager manager = createCostBenchDbManager()) {
            switch (type.toLowerCase()) {
            case BenchConst.ONLINE_RANDOM:
                main0(batchDate, manager);
                break;
            case BenchConst.ONLINE_SCHEDULE:
                mainSchedule(batchDate, manager);
                break;
            default:
                throw new IllegalArgumentException(MessageFormat.format("invalid online.type ({0})", type));
            }
        }
    }

    public static CostBenchDbManager createCostBenchDbManager() {
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
        return CostBenchDbManager.createInstance(type, isolationLevel, isMultiSession);
    }

    // main0

    private static void main0(LocalDate batchDate, CostBenchDbManager manager) {
        int threadSize = BenchConst.onlineThreadSize();

        AtomicBoolean stopRequest = new AtomicBoolean(false);
        List<CostAccountingOnlineThread> threadList = createThread(threadSize, manager, batchDate, stopRequest);

        ExExecutorService pool = newExecutorService(threadList.size(), stopRequest);
        AtomicBoolean done = new AtomicBoolean(false);
        try {
            Runtime.getRuntime().addShutdownHook(new Thread() {
                @Override
                public void run() {
                    LOG.debug("shutdown-hook start");
                    stopRequest.set(true);

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

            pool.invokeAll(threadList);
        } catch (InterruptedException e) {
            LOG.debug("InterruptedException", e);
        } finally {
            try {
                pool.shutdownNow();

                // 例外出力
                pool.printException();

                LOG.info("Counter infos: \n---\n{}---", CostBenchDbManager.createCounterReport());
            } finally {
                done.set(true);
            }
        }
    }

    private static List<CostAccountingOnlineThread> createThread(int threadSize, CostBenchDbManager manager, LocalDate batchDate, AtomicBoolean stopRequest) {
        List<CostAccountingOnlineThread> threadList = new ArrayList<>(threadSize);

        List<Integer> factoryList = getAllFactory(manager);
        for (int i = 0; i < threadSize; i++) {
            create1(manager, threadList, factoryList, batchDate, stopRequest);
        }

        return threadList;
    }

    private static List<Integer> getAllFactory(CostBenchDbManager manager) {
        FactoryMasterDao dao = manager.getFactoryMasterDao();

        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC().label("select factory"));
        try {
            return manager.execute(setting, dao::selectAllId);
        } finally {
            CostBenchDbManager.initCounter();
        }
    }

    private static int threadId = 0;

    private static void create1(CostBenchDbManager manager, List<CostAccountingOnlineThread> threadList, List<Integer> factoryList, LocalDate date, AtomicBoolean stopRequest) {
        LOG.info("create thread{}", threadId);
        CostAccountingOnlineThread thread = new CostAccountingOnlineThread(threadId++, manager, factoryList, date, stopRequest);
        threadList.add(thread);
    }

    static ExExecutorService newExecutorService(int size, AtomicBoolean stopRequest) {
        // return Executors.newFixedThreadPool(size);
        return new ExExecutorService(size, stopRequest);
    }

    private static class ExExecutorService extends ThreadPoolExecutor {
        private final AtomicBoolean stopRequest;
        private final List<Exception> exceptionList = new CopyOnWriteArrayList<>();

        public ExExecutorService(int nThreads, AtomicBoolean stopRequest) {
            super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
            this.stopRequest = stopRequest;
        }

        @Override
        protected void afterExecute(Runnable r, Throwable t) {
            Future<?> task = (Future<?>) r;
            try {
                task.get();
            } catch (InterruptedException | CancellationException ignore) {
                stopRequest.set(true);
            } catch (Exception e) {
                exceptionList.add(e);
                stopRequest.set(true);
            }
        }

        public void printException() {
            for (Exception e : exceptionList) {
                LOG.error("thread exception report", e);
            }
        }
    }

    // main(schedule)

    private static void mainSchedule(LocalDate batchDate, CostBenchDbManager manager) throws Exception {
        // @see com.tsurugidb.benchmark.phonebill.app.billing.PhoneBill#execute()

        var onlineList = createOnlineApps(batchDate, manager);

        AtomicBoolean wait = new AtomicBoolean(true);
        AtomicBoolean done = new AtomicBoolean(false);
        try {
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

            executeOnlineApp(onlineList, wait);
        } finally {
            done.set(true);
        }
    }

    private static List<BenchOnlineSchedule> createOnlineApps(LocalDate batchDate, CostBenchDbManager manager) {
        var factoryList = getAllFactory(manager);

        var taskList = new ArrayList<Supplier<BenchOnlineTask>>();
        taskList.add(BenchOnlineNewItemTask::new);
        taskList.add(BenchOnlineUpdateManufacturingTask::new);
        taskList.add(BenchOnlineUpdateMaterialTask::new);
        taskList.add(BenchOnlineUpdateCostTask::new);
        taskList.add(BenchOnlineShowWeightTask::new);
        taskList.add(BenchOnlineShowQuantityTask::new);
        taskList.add(BenchOnlineShowCostTask::new);

        var onlineList = new ArrayList<BenchOnlineSchedule>();
        for (var taskSupplier : taskList) {
            var task = taskSupplier.get();
            String taskName = task.getTitle();
            int size = BenchConst.onlineThreadSize(taskName);

            for (int i = 0; i < size; i++, task = null) {
                if (task == null) {
                    task = taskSupplier.get();
                }
                task.setDao(manager);

                var onlineApp = new BenchOnlineSchedule(task, i, factoryList, batchDate);
                onlineList.add(onlineApp);
            }
        }

        return onlineList;
    }

    private static void executeOnlineApp(List<BenchOnlineSchedule> onlineList, AtomicBoolean wait) throws Exception {
        final ExecutorService service = onlineList.isEmpty() ? null : Executors.newFixedThreadPool(onlineList.size());
        try {
            CostBenchDbManager.initCounter();

            // オンラインアプリを実行する
            onlineList.parallelStream().forEach(task -> service.submit(task));

            while (wait.get()) {
                Thread.sleep(100);
            }
        } finally {
            // オンラインアプリを終了する
            onlineList.forEach(task -> task.terminate());
            if (service != null) {
                service.shutdown();
                service.awaitTermination(5, TimeUnit.MINUTES);
            }
            LOG.info("Counter infos: \n---\n{}---", CostBenchDbManager.createCounterReport());
        }
    }
}
