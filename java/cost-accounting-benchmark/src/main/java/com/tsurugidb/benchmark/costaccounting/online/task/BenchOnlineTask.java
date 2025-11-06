/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.online.task;

import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.ConsoleType;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public abstract class BenchOnlineTask extends BenchTask implements Closeable {

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

    private static final Map<String, Optional<BufferedWriter>> debugWriterMap = new ConcurrentHashMap<>();

    protected void writeDebugFile(TgTmSetting setting, Supplier<String> supplier) {
        var debugWriterOpt = debugWriterMap.computeIfAbsent(title, k -> {
            String s = BenchConst.onlineDebugDir(title);
            if (s == null) {
                return Optional.empty();
            }
            try {
                var dir = Path.of(s);
                Files.createDirectories(dir);
                String desc = setting.getTransactionOptionSupplier().getDescription();
                var path = dir.resolve(config.getLabel() + "." + desc + "." + title + ".tsv");
                LOG.info("debugWriter: {}", path);
                return Optional.of(Files.newBufferedWriter(path, StandardCharsets.UTF_8));
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        });
        debugWriterOpt.ifPresent(writer -> {
            String s = supplier.get();
            try {
                writer.write(s + "\n");
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        });
    }

    public static void closeDebugWriter() {
        Logger log = LoggerFactory.getLogger(BenchOnlineTask.class);
        for (var debugWriterOpt : debugWriterMap.values()) {
            debugWriterOpt.ifPresent(writer -> {
                try {
                    writer.close();
                } catch (IOException e) {
                    log.warn("debugWriter close error", e);
                }
            });
        }
        debugWriterMap.clear();
    }

    private long sleepTime = Integer.MIN_VALUE;

    public long getSleepTime() {
        if (sleepTime < 0) {
            this.sleepTime = TimeUnit.SECONDS.toMillis(BenchConst.onlineRandomTaskSleepTime(title));
        }
        return sleepTime;
    }

    @Override
    public void close() {
        // do override
    }
}
