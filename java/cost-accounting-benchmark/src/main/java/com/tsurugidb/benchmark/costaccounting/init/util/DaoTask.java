package com.tsurugidb.benchmark.costaccounting.init.util;

import java.util.concurrent.ForkJoinTask;
import java.util.concurrent.atomic.AtomicInteger;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

@SuppressWarnings("serial")
public abstract class DaoTask extends ForkJoinTask<Void> {

    private final CostBenchDbManager dbManager;
    private final TgTmSetting setting;
    private final AtomicInteger insertCount = new AtomicInteger();

    public DaoTask(CostBenchDbManager dbManager, TgTmSetting setting) {
        this.dbManager = dbManager;
        this.setting = setting;
    }

    @Override
    public final Void getRawResult() {
        return null;
    }

    @Override
    protected final void setRawResult(Void value) {
        return;
    }

    @Override
    protected final boolean exec() {
        dbManager.execute(setting, () -> {
            insertCount.set(0);
            execute(insertCount);
        });
        return true;
    }

    protected abstract void execute(AtomicInteger insertCount);

    public int getInsertCount() {
        return insertCount.get();
    }
}
