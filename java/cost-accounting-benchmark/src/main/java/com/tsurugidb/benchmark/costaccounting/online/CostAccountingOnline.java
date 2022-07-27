package com.tsurugidb.benchmark.costaccounting.online;

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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

public class CostAccountingOnline {
    private static final Logger LOG = LoggerFactory.getLogger(CostAccountingOnline.class);

    public static void main(String[] args) {
        try (CostBenchDbManager manager = createCostBenchDbManager()) {
            main0(args, manager);
        }
    }

    public static CostBenchDbManager createCostBenchDbManager() {
        int type = BenchConst.onlineJdbcType();
        return CostBenchDbManager.createInstance(type);
    }

    private static void main0(String[] args, CostBenchDbManager manager) {
        LocalDate batchDate;
        try {
            batchDate = LocalDate.parse(args[0]);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid batchDate (args[0])", e);
        }
        int threadSize;
        try {
            threadSize = Integer.parseInt(args[1]);
        } catch (Exception e) {
            throw new IllegalArgumentException("invalid threadSize (args[1])", e);
        }

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
            e.printStackTrace();
        } finally {
            try {
                pool.shutdownNow();

                // 例外出力
                pool.printException();
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

        var setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
        return manager.execute(setting, () -> {
            return dao.selectAllId();
        });
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
                LOG.error("exception", e);
            }
        }
    }
}
