package com.tsurugidb.benchmark.costaccounting.online.task;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.ConsoleType;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;

public abstract class BenchOnlineTask extends BenchTask {

    public static final List<String> TASK_NAME_LIST = List.of( //
            BenchOnlineNewItemTask.TASK_NAME, //
            BenchOnlineUpdateManufacturingTask.TASK_NAME, //
            BenchOnlineUpdateMaterialTask.TASK_NAME, //
            BenchOnlineUpdateCostAddTask.TASK_NAME, //
            BenchOnlineUpdateCostSubTask.TASK_NAME, //
            BenchOnlineShowWeightTask.TASK_NAME, //
            BenchOnlineShowQuantityTask.TASK_NAME, //
            BenchOnlineShowCostTask.TASK_NAME //
    );

    private static final ConsoleType CONSOLE_TYPE = BenchConst.onlineConsoleType();

    private int threadId;
    private BufferedWriter writer;

    protected int factoryId;
    protected LocalDate date;

    protected final BenchRandom random = new BenchRandom();

    public BenchOnlineTask(String title, int taskId) {
        super(title, taskId);
    }

    public void initializeForRandom(int threadId, BufferedWriter writer) {
        this.threadId = threadId;
        this.writer = writer;
    }

    public void initialize(int factoryId, LocalDate date) {
        this.factoryId = factoryId;
        this.date = date;
    }

    public final void execute() {
        incrementStartCounter();
        logStart("factory=%d, date=%s", factoryId, date);

        boolean exists;
        try {
            exists = execute1();
        } catch (Throwable e) {
            incrementFailCounter();
            throw e;
        }

        String target;
        if (exists) {
            incrementSuccessCounter();
            target = "exists";
        } else {
            incrementNothingCounter();
            target = "nothing";
        }
        logEnd("factory=%d, date=%s target=%s", factoryId, date, target);
        logFlush();
    }

    protected abstract boolean execute1();

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
        switch (CONSOLE_TYPE) {
        case NULL:
            return;
        case STDOUT:
            System.out.println(s);
            break;
        default:
            throw new AssertionError(CONSOLE_TYPE);
        }
    }

    private long sleepTime = Integer.MIN_VALUE;

    public long getSleepTime() {
        if (sleepTime < 0) {
            this.sleepTime = TimeUnit.SECONDS.toMillis(BenchConst.onlineRandomTaskSleepTime(title));
        }
        return sleepTime;
    }
}
