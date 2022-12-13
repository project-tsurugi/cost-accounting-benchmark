package com.tsurugidb.benchmark.costaccounting.time.table;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.time.TimeRecord;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public abstract class TableTime {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    protected final String tableName;
    protected final int size;
    protected CostBenchDbManager dbManager;
    private IsolationLevel isolation;
    private String option;
    private List<TimeRecord> records;

    public TableTime(String tableName) {
        this.tableName = tableName;
        this.size = BenchConst.timeCommandSize(tableName);
    }

    public void initialize(CostBenchDbManager dbManager, IsolationLevel isolation, String option, List<TimeRecord> records) {
        this.dbManager = dbManager;
        this.isolation = isolation;
        this.option = option;
        this.records = records;
    }

    public abstract void execute();

    protected void execute(String sqlName, int count, Runnable action) {
        for (int i = 0; i < count; i++) {
            execute(sqlName, action);
        }
    }

    protected void execute(String sqlName, Runnable action) {
        var txOption = getOption();
        var setting = TgTmSetting.ofAlways(txOption);

        var record = new TimeRecord(isolation, option, tableName, size, sqlName);
        records.add(record);
        LOG.info("Executing with {}.", record.getParamString());

        record.start();
        dbManager.execute(setting, () -> {
            record.startMain();
            action.run();
            record.beforeCommit();
        });
        record.finish();

        LOG.info("Finished. elapsed secs = {}.", record.elapsedMillis() / 1000.0);
    }

    private TgTxOption getOption() {
        switch (option) {
        case "OCC":
            return TgTxOption.ofOCC();
        default:
            return TgTxOption.ofLTX(tableName);
        }
    }
}
