package com.tsurugidb.benchmark.costaccounting.online;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.UncheckedIOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineNewItemTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineShowCostTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineShowQuantityTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineShowWeightTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateCostTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateManufacturingTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchOnlineUpdateMaterialTask;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;

public class CostAccountingOnlineThread implements Runnable, Callable<Void> {
    private static final Logger LOG = LoggerFactory.getLogger(CostAccountingOnlineThread.class);

    private final int threadId;
    private final CostBenchDbManager dbManager;
    private final List<Integer> factoryList;
    private final LocalDate date;
    private final AtomicBoolean stopRequest;

    private final List<BenchOnlineTask> taskList = new ArrayList<>();
    private final NavigableMap<Integer, BenchOnlineTask> taskRatioMap = new TreeMap<>();
    private final int taskRatioMax;

    private final BenchRandom random = new BenchRandom();

    public CostAccountingOnlineThread(int id, CostBenchDbManager dbManager, List<Integer> factoryList, LocalDate date, AtomicBoolean stopRequest) {
        this.threadId = id;
        this.dbManager = dbManager;
        this.factoryList = factoryList;
        this.date = date;
        this.stopRequest = stopRequest;

        taskList.add(new BenchOnlineNewItemTask());
        taskList.add(new BenchOnlineUpdateManufacturingTask());
        taskList.add(new BenchOnlineUpdateMaterialTask());
        taskList.add(new BenchOnlineUpdateCostTask());

        taskList.add(new BenchOnlineShowWeightTask());
        taskList.add(new BenchOnlineShowQuantityTask());
        taskList.add(new BenchOnlineShowCostTask());

        this.taskRatioMax = initializeTaskRatio(taskList);
    }

    private int initializeTaskRatio(List<BenchOnlineTask> taskList) {
        int sum = 0;
        for (BenchOnlineTask task : taskList) {
            String title = task.getTitle();
            int ratio = BenchConst.onlineTaskRatio(title);
            if (ratio > 0) {
                sum += ratio;
                taskRatioMap.put(sum, task);
            }
        }
        return sum;
    }

    @Override
    public void run() {
        for (BenchOnlineTask task : taskList) {
            task.setDao(dbManager);
        }

        Path path = BenchConst.onlineLogFilePath(threadId);
        try (BufferedWriter writer = Files.newBufferedWriter(path)) {
            for (;;) {
                if (stopRequest.get()) {
                    break;
                }
                try {
                    if (!execute1(writer)) {
                        break;
                    }
                } catch (Throwable t) {
                    stopRequest.set(true);
                    try {
                        PrintWriter pw = new PrintWriter(writer);
                        t.printStackTrace(pw);
                    } catch (Throwable s) {
                        t.addSuppressed(s);
                    }

                    String message = t.getMessage();
                    if (message == null) {
                        message = t.getClass().getName();
                    }
                    LOG.error("thread{} abend. error={}", threadId, message);
                    throw t;
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }

        LOG.info("thread{} end", threadId);
    }

    private boolean execute1(BufferedWriter writer) {
        int factoryId = factoryList.get(random.nextInt(factoryList.size()));

        BenchOnlineTask task = getTaskRandom();
        task.initialize(threadId, writer);
        task.initialize(factoryId, date);

        task.execute();

        if (Thread.interrupted() || stopRequest.get()) {
            return false;
        }

        long sleepTime = task.getSleepTime();
        if (sleepTime > 0) {
            try {
                Thread.sleep(sleepTime);
            } catch (InterruptedException e) {
                return false;
            }
        }

        return true;
    }

    private BenchOnlineTask getTaskRandom() {
        int i = random.nextInt(taskRatioMax);
        BenchOnlineTask task = taskRatioMap.higherEntry(i).getValue();
        return task;
    }

    @Override
    public Void call() throws Exception {
        run();
        return null;
    }
}
