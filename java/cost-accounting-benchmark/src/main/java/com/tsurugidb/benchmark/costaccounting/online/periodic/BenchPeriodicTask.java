package com.tsurugidb.benchmark.costaccounting.online.periodic;

import java.time.LocalDate;

import com.tsurugidb.benchmark.costaccounting.online.task.BenchTask;

public abstract class BenchPeriodicTask extends BenchTask {

    protected LocalDate date;

    public BenchPeriodicTask(String tableName) {
        super(tableName);
    }

    public void initialize(LocalDate date) {
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
