package com.tsurugidb.benchmark.costaccounting.online.task;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public abstract class BenchOnlineTask {

    private final String title;

    protected CostBenchDbManager dbManager;

    private int threadId;
    private BufferedWriter writer;
    private AtomicBoolean terminationRequested;

    protected int factoryId;
    protected LocalDate date;

    protected final BenchRandom random = new BenchRandom();

    public BenchOnlineTask(String title) {
        this.title = title;
    }

    public String getTitle() {
        return title;
    }

    protected final TgTmSetting getSetting(Supplier<TgTxOption> ltxSupplier) {
        TgTmSetting setting = createSetting(ltxSupplier);
        setting.transactionLabel(title);
        return setting;
    }

    private TgTmSetting createSetting(Supplier<TgTxOption> ltxSupplier) {
        String option = BenchConst.onlineTsurugiTxOption(title).toUpperCase();
        switch (option) {
        case "OCC":
        default:
            return TgTmSetting.ofAlways(TgTxOption.ofOCC());
        case "LTX":
            return TgTmSetting.ofAlways(ltxSupplier.get());
        case "MIX":
            return TgTmSetting.of(TgTxOption.ofOCC(), ltxSupplier.get());
        }
    }

    public void setDao(CostBenchDbManager dbManager) {
        this.dbManager = dbManager;
    }

    public void initialize(int threadId, BufferedWriter writer, AtomicBoolean terminationRequested) {
        this.threadId = threadId;
        this.writer = writer;
        this.terminationRequested = terminationRequested;
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
        logFlush();
    }

    protected abstract boolean execute1();

    protected final void checkStop() {
        if (terminationRequested.get()) {
            throw new RuntimeException("stop by request. task=" + title);
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
        if (this.writer == null) {
            return;
        }

        String start = startDateTime.toString();
        String now = (nowDateTime != null) ? nowDateTime.toString() : "                       ";
        String s = "thread" + threadId + " " + title + " " + sub + " " + start + "/" + now + " " + message + "\n";
        try {
            writer.write(s);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    private void logFlush() {
        if (this.writer == null) {
            return;
        }

        try {
            writer.flush();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
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
        return CostAccountingOnline.createDbManager();
    }
}
