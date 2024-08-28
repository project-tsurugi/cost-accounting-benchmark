/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.benchmark.costaccounting.online.task;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public abstract class BenchTask {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());

    public static void clearPrepareDataAll() {
        BenchOnlineNewItemTask.clearPrepareData();
        BenchOnlineUpdateManufacturingTask.clearPrepareData();
        BenchOnlineUpdateMaterialTask.clearPrepareData();
    }

    protected final String title;
    protected final int taskId;

    protected OnlineConfig config;
    protected CostBenchDbManager dbManager;

    private long startTime;

    public BenchTask(String title, int taskId) {
        this.title = title;
        this.taskId = taskId;
    }

    public String getTitle() {
        return title;
    }

    public void setDao(OnlineConfig config, CostBenchDbManager dbManager) {
        this.config = config;
        this.dbManager = dbManager;
    }

    public abstract void initializeSetting();

    protected final void setTxOptionDescription(TgTmSetting setting) {
        String description = setting.getTransactionOptionSupplier().getDescription();
        dbManager.setTxOptionDescription(title, description);
    }

    public void executePrepare() {
        // do override
    }

    protected final void incrementStartCounter() {
        this.startTime = System.nanoTime();
        dbManager.incrementTaskCounter(title, CounterName.TASK_START);
    }

    protected final void incrementNothingCounter() {
        long endTime = System.nanoTime();
        dbManager.incrementTaskCounter(title, CounterName.TASK_NOTHING);
        dbManager.addTaskTime(title, CounterName.TASK_NOTHING, endTime - startTime);
    }

    protected final void incrementSuccessCounter() {
        long endTime = System.nanoTime();
        dbManager.incrementTaskCounter(title, CounterName.TASK_SUCCESS);
        dbManager.addTaskTime(title, CounterName.TASK_SUCCESS, endTime - startTime);
    }

    protected final void incrementFailCounter() {
        long endTime = System.nanoTime();
        dbManager.incrementTaskCounter(title, CounterName.TASK_FAIL);
        dbManager.addTaskTime(title, CounterName.TASK_FAIL, endTime - startTime);
    }

    protected static final CostBenchDbManager createCostBenchDbManagerForTest() {
        var config = CostAccountingOnline.createDefaultConfig(InitialData.DEFAULT_BATCH_DATE);
        return CostAccountingOnline.createDbManager(config);
    }
}
