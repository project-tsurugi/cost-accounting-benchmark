package com.tsurugidb.benchmark.costaccounting.online.periodic;

import java.time.LocalDate;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.online.task.BenchTask;

public abstract class BenchPeriodicTask extends BenchTask {

    public static final List<String> TASK_NAME_LIST = List.of( //
            BenchPeriodicUpdateStockTask.TASK_NAME //
    );

    protected List<Integer> factoryList;
    protected LocalDate date;

    public BenchPeriodicTask(String tableName) {
        super(tableName);
    }

    public void initialize(List<Integer> factoryList, LocalDate date) {
        this.factoryList = factoryList;
        this.date = date;
    }

    public final void execute() {
        incrementStartCounter();

        boolean exists;
        try {
            exists = execute1();
        } catch (Throwable e) {
            incrementFailCounter();
            throw e;
        }

        if (exists) {
            incrementSuccessCounter();
        } else {
            incrementNothingCounter();
        }
    }

    protected abstract boolean execute1();
}
