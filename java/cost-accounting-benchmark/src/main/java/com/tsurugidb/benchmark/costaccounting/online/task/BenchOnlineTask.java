package com.tsurugidb.benchmark.costaccounting.online.task;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public abstract class BenchOnlineTask {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    private final String title;

    protected CostBenchDbManager dbManager;

    private int threadId;
    private BufferedWriter writer;

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
        String option = BenchConst.onlineTsurugiTxOption(title);
        String head = option.substring(0, 3).toUpperCase();
        switch (head) {
        case "OCC":
        default:
            onceLog(() -> LOG.info("txOption: OCC"), "OCC");
            return TgTmSetting.ofAlways(TgTxOption.ofOCC());
        case "LTX":
        case "RTX":
            var txOption = ltxSupplier.get();
            onceLog(() -> LOG.info("txOption: {}", txOption.typeName()), txOption.typeName());
            return TgTmSetting.ofAlways(txOption);
        case "MIX":
            int size1 = 3, size2 = 2;
            String rest = option.substring(3);
            String[] ss = rest.split("-");
            try {
                size1 = Integer.parseInt(ss[0].trim());
            } catch (NumberFormatException e) {
                LOG.warn("online.tsurugi.tx.option(MIX) error", e);
            }
            if (ss.length > 1) {
                try {
                    size2 = Integer.parseInt(ss[1].trim());
                } catch (NumberFormatException e) {
                    LOG.warn("online.tsurugi.tx.option(MIX) error", e);
                }
            }
            var txOption2 = ltxSupplier.get();
            int finalSize1 = size1, finalSize2 = size2;
            onceLog(() -> LOG.info("txOptionSupplier: OCC*{}, {}*{}", finalSize1, txOption2.typeName(), finalSize2), txOption2.typeName());
            return TgTmSetting.of(TgTxOption.ofOCC(), size1, txOption2, size2);
        }
    }

    private static final Map<String, Boolean> ONCE_LOG_MAP = new ConcurrentHashMap<>();

    private void onceLog(Runnable action, String typeName) {
        if (BenchConst.dbmsType() != DbmsType.TSURUGI) {
            return;
        }

        String key = getClass().getName() + "." + typeName;
        if (ONCE_LOG_MAP.putIfAbsent(key, Boolean.TRUE) == null) {
            action.run();
        }
    }

    public static void clearOnceLog() {
        ONCE_LOG_MAP.clear();
    }

    public void setDao(CostBenchDbManager dbManager) {
        this.dbManager = dbManager;
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
        logStart("factory=%d, date=%s", factoryId, date);

        boolean result = execute1();
        String target = result ? "exists" : "nothing";

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
