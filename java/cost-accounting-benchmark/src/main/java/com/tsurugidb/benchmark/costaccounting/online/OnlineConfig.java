package com.tsurugidb.benchmark.costaccounting.online;

import java.time.LocalDate;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.slf4j.Logger;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.online.periodic.BenchPeriodicTask;
import com.tsurugidb.benchmark.costaccounting.online.task.BenchTask;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class OnlineConfig {

    private String label;
    private final LocalDate batchDate;
    private IsolationLevel isolationLevel;
    private boolean isMultiSession = true;
    private final Map<String, String> optionMap = new HashMap<>();
    private final Map<String, Integer> threadSizeMap = new HashMap<>();
    private int executeTime;

    public OnlineConfig(LocalDate batchDate) {
        this.batchDate = batchDate;
    }

    public void setLabel(String label) {
        this.label = label;
    }

    public String getLabel() {
        return this.label;
    }

    public LocalDate getBatchDate() {
        return this.batchDate;
    }

    public void setIsolationLevel(IsolationLevel isolationLevel) {
        this.isolationLevel = isolationLevel;
    }

    public IsolationLevel getIsolationLevel() {
        return this.isolationLevel;
    }

    public void setMultiSession(boolean isMultiSession) {
        this.isMultiSession = isMultiSession;
    }

    public boolean isMultiSession() {
        return this.isMultiSession;
    }

    public void setTxOption(String taskName, String option) {
        optionMap.put(taskName, option);
    }

    public String getTxOption(String taskName) {
        return optionMap.get(taskName);
    }

    public String getTxOption(BenchTask task) {
        var option = getTxOption(task.getTitle());
        if (option == null) {
            String defaultKey = (task instanceof BenchPeriodicTask) ? "periodic" : "online";
            option = getTxOption(defaultKey);
            if (option == null) {
                throw new IllegalStateException("not found txOption. taskName=" + task.getTitle());
            }
        }
        return option;
    }

    public TgTmSetting getSetting(Logger log, BenchTask task, Supplier<TgTxOption> ltxSupplier) {
        TgTmSetting setting = createSetting(log, task, ltxSupplier);
        setting.transactionLabel(task.getTitle());
        return setting;
    }

    private TgTmSetting createSetting(Logger log, BenchTask task, Supplier<TgTxOption> ltxSupplier) {
        TgTmSetting setting;
        String description;

        String option = getTxOption(task);
        String head = option.substring(0, 3).toUpperCase();
        switch (head) {
        case "OCC":
        default:
            description = "OCC";
            onceLog(task, () -> log.info("txOption: {}", description), "OCC");
            setting = TgTmSetting.ofAlways(TgTxOption.ofOCC());
            break;
        case "LTX":
        case "RTX":
            var txOption = ltxSupplier.get();
            description = txOption.typeName();
            onceLog(task, () -> log.info("txOption: {}", description), txOption.typeName());
            setting = TgTmSetting.ofAlways(txOption);
            break;
        case "MIX":
            int size1 = 3, size2 = 2;
            String rest = option.substring(3);
            String[] ss = rest.split("-");
            try {
                size1 = Integer.parseInt(ss[0].trim());
            } catch (NumberFormatException e) {
                log.warn("online.tsurugi.tx.option(MIX) error", e);
            }
            if (ss.length > 1) {
                try {
                    size2 = Integer.parseInt(ss[1].trim());
                } catch (NumberFormatException e) {
                    log.warn("online.tsurugi.tx.option(MIX) error", e);
                }
            }
            var txOption2 = ltxSupplier.get();
            description = String.format("OCC*%d, %s*%d", size1, txOption2.typeName(), size2);
            onceLog(task, () -> log.info("txOptionSupplier: {}", description), txOption2.typeName());
            setting = TgTmSetting.of(TgTxOption.ofOCC(), size1, txOption2, size2);
            break;
        }

        setting.getTransactionOptionSupplier().setDescription(description);
        return setting;
    }

    private final Map<String, Boolean> onceLogMap = new ConcurrentHashMap<>();

    private void onceLog(BenchTask task, Runnable action, String typeName) {
        if (BenchConst.dbmsType() != DbmsType.TSURUGI) {
            return;
        }

        String key = task.getTitle() + "." + typeName;
        if (onceLogMap.putIfAbsent(key, Boolean.TRUE) == null) {
            action.run();
        }
    }

    public void setThreadSize(String taskName, int threadSize) {
        threadSizeMap.put(taskName, threadSize);
    }

    public int getThreadSize(String taskName) {
        var threadSize = threadSizeMap.get(taskName);
        if (threadSize == null) {
            throw new IllegalStateException("not found threadSize. taskName=" + taskName);
        }
        return threadSize;
    }

    public void setExecuteTime(int time) {
        this.executeTime = time;
    }

    public int getExecuteTime() {
        return this.executeTime;
    }
}
