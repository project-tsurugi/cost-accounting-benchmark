package com.tsurugidb.benchmark.costaccounting.online.task;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;

public abstract class BenchOnlineTask {

    private final String title;

    protected CostBenchDbManager dbManager;

    private int threadId;
    private BufferedWriter writer;
    private AtomicBoolean stopRequest;

    protected int factoryId;
    protected LocalDate date;

    protected final BenchRandom random = new BenchRandom();

    public BenchOnlineTask(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    public void setDao(CostBenchDbManager dbManager) {
        this.dbManager = dbManager;
    }

    public void initialize(int threadId, BufferedWriter writer, AtomicBoolean stopRequest) {
        this.threadId = threadId;
        this.writer = writer;
        this.stopRequest = stopRequest;
    }

    public void initialize(int factoryId, LocalDate date) {
        this.factoryId = factoryId;
        this.date = date;
    }

    public final void execute() {
        logStart("factory=%d, date=%s", factoryId, date);

        boolean result = execute1();
        String target = result ? "exists" : "nothing";

        logEnd("factory=%d, date=%s target=%s", factoryId, date, target);
    }

    protected abstract boolean execute1();

    protected void checkStop() {
        if (stopRequest.get()) {
            throw new RuntimeException("stop by request");
        }
    }

    private LocalDateTime startDateTime, nowDateTime;

    protected void logStart(String format, Object... args) {
        this.startDateTime = LocalDateTime.now();
        this.nowDateTime = null;
        String s = String.format(format, args);
        logTime("start ", s);
    }

    protected void logTarget(String format, Object... args) {
        this.nowDateTime = LocalDateTime.now();
        String s = String.format(format, args);
        logTime("target", s);
    }

    protected void logEnd(String format, Object... args) {
        this.nowDateTime = LocalDateTime.now();
        String s = String.format(format, args);
        logTime("end   ", s);
    }

    protected void logTime(String sub, String message) {
        String start = startDateTime.toString();
        String now = (nowDateTime != null) ? nowDateTime.toString() : "                       ";
        String s = "thread" + threadId + " " + title + " " + sub + " " + start + "/" + now + " " + message + "\n";
        try {
            writer.write(s);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    protected void console(String format, Object... args) {
        String s = String.format(format, args);
        System.out.println(s);
    }

    private long sleepTime = Integer.MIN_VALUE;

    public long getSleepTime() {
        if (sleepTime < 0) {
            this.sleepTime = TimeUnit.SECONDS.toMillis(BenchConst.onlineTaskSleepTime(title));
        }
        return sleepTime;
    }

    protected static final CostBenchDbManager createCostBenchDbManagerForTest() {
        return CostAccountingOnline.createCostBenchDbManager();
    }
}
