package com.tsurugidb.benchmark.costaccounting.online.periodic;

import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class CostAccountingPeriodicAppSchedule implements Runnable {
    private static final Logger LOG = LoggerFactory.getLogger(CostAccountingPeriodicAppSchedule.class);

    /**
     * 終了リクエストの有無を表すフラグ
     */
    private final AtomicBoolean terminationRequested;

    /**
     * タスク名(=スレッド名)
     */
    private String name;

    private final BenchPeriodicTask periodicTask;

    private final long interval;

    public CostAccountingPeriodicAppSchedule(BenchPeriodicTask task, int threadId, List<Integer> factoryList, LocalDate date, AtomicBoolean terminationRequested) {
        this.periodicTask = task;
        task.initialize(date);
        this.terminationRequested = terminationRequested;

        this.name = "periodic." + task.getTitle() + "." + threadId;
        this.interval = BenchConst.onlineInterval(task.getTitle());
    }

    @Override
    public void run() {
        try {
            Thread.currentThread().setName(name);

            LOG.info("{} started.", name);
            while (!terminationRequested.get()) {
                schedule();
            }
            LOG.info("{} terminated.", name);
        } catch (Throwable e) {
            LOG.error("Aborting by exception", e);
            throw e;
        }
    }

    private void schedule() {
        long startTime = System.currentTimeMillis();

        execute();

        // sleep
        while (!terminationRequested.get()) {
            long now = System.currentTimeMillis();
            if (now - startTime >= interval) {
                break;
            }
            try {
                Thread.sleep(10);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }
    }

    private void execute() {
        for (;;) { // 処理に成功するまで無限にリトライする
            if (terminationRequested.get()) {
                return;
            }
            try {
                periodicTask.execute();
                return;
            } catch (RuntimeException e) {
                LOG.info("Caught exception, retrying... ", e);
            }
        }
    }

    public void terminate() {
        terminationRequested.set(true);
    }
}
