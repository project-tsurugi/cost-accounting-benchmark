package com.tsurugidb.benchmark.costaccounting.init.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

@SuppressWarnings("serial")
public abstract class DaoListTask<T> extends RecursiveAction {

    private final CostBenchDbManager dbManager;
    private final TgTmSetting setting;

    private final List<T> list = new ArrayList<>();

    public DaoListTask(CostBenchDbManager dbManager, TgTmSetting setting) {
        this.dbManager = dbManager;
        this.setting = setting;
    }

    public void add(T t) {
        list.add(t);
    }

    public int size() {
        return list.size();
    }

    @Override
    protected final void compute() {
        dbManager.execute(setting, () -> {
            for (T t : list) {
                execute(t);
            }
        });
    }

    protected abstract void execute(T t);
}
