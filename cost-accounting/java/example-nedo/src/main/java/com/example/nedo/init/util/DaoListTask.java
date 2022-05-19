package com.example.nedo.init.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;

import com.example.nedo.jdbc.CostBenchDbManager;

@SuppressWarnings("serial")
public abstract class DaoListTask<T> extends RecursiveAction {

    private final CostBenchDbManager dbManager;

    private final List<T> list = new ArrayList<>();

    public DaoListTask(CostBenchDbManager dbManager) {
        this.dbManager = dbManager;
    }

    public void add(T t) {
        list.add(t);
    }

    public int size() {
        return list.size();
    }

    @Override
    protected final void compute() {
        dbManager.execute(() -> {
            for (T t : list) {
                execute(t);
            }
        });
    }

    protected abstract void execute(T t);
}
