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
package com.tsurugidb.benchmark.costaccounting.online.periodic;

import java.io.Closeable;
import java.time.LocalDate;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.tsurugidb.benchmark.costaccounting.online.task.BenchTask;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public abstract class BenchPeriodicTask extends BenchTask implements Closeable {

    public static final List<String> TASK_NAME_LIST = List.of( //
            BenchPeriodicUpdateStockTask.TASK_NAME //
    );

    protected List<Integer> factoryList;
//  protected LocalDate date;

    private final int capSize;

    private int executeCount = 0;

    public BenchPeriodicTask(String taskName, int taskId) {
        super(taskName, taskId);

        this.capSize = BenchConst.periodicCapSize(taskName);
        if (capSize >= 0) {
            LOG.info("cap.size={}", capSize);
        }
    }

    public void initialize(List<Integer> factoryList, LocalDate date) {
        this.factoryList = factoryList;
//      this.date = date;
    }

    public final void execute() {
        if (!canExecute()) {
            try {
                TimeUnit.SECONDS.sleep(1);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
            return;
        }

        incrementStartCounter();
        executeCount++;

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

    private boolean capLog = false;

    protected boolean canExecute() {
        if (capSize < 0) {
            return true;
        }

        boolean isExecute = executeCount < capSize;
        if (!isExecute) {
            if (!capLog) {
                capLog = true;
                LOG.info("executeCount has reached the limit. executeCount={}, capSize={}", executeCount, capSize);
            }
        }
        return isExecute;
    }

    protected abstract boolean execute1();

    @Override
    public abstract void close();
}
