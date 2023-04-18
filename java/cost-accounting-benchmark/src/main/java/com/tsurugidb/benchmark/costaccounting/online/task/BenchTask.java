package com.tsurugidb.benchmark.costaccounting.online.task;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public abstract class BenchTask {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    protected final String title;

    protected CostBenchDbManager dbManager;

    public BenchTask(String title) {
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

    protected final void incrementStartCounter() {
        dbManager.incrementTaskCounter(title, CounterName.TASK_START);
    }

    protected final void incrementNothingCounter() {
        dbManager.incrementTaskCounter(title, CounterName.TASK_NOTHING);
    }

    protected final void incrementSuccessCounter() {
        dbManager.incrementTaskCounter(title, CounterName.TASK_SUCCESS);
    }

    protected final void incrementFailCounter() {
        dbManager.incrementTaskCounter(title, CounterName.TASK_FAIL);
    }

    protected static final CostBenchDbManager createCostBenchDbManagerForTest() {
        return CostAccountingOnline.createDbManager();
    }
}
