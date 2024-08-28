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
package com.tsurugidb.benchmark.costaccounting.time.table;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.time.TimeRecord;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public abstract class TableTime {
    private final Logger LOG = LoggerFactory.getLogger(getClass());

    protected final String tableName;
    protected final int size;
    protected int sizeAdjustStart;
    protected int sizeAdjustEnd;
    private DbmsType dbmsType;
    protected CostBenchDbManager dbManager;
    private IsolationLevel isolation;
    private String option;
    private List<TimeRecord> records;

    public TableTime(String tableName) {
        this.tableName = tableName;
        this.size = BenchConst.timeCommandSize(tableName);
        int adjustStart = BenchConst.timeCommandSizeAdjust(tableName, "start");
        int adjustEnd = BenchConst.timeCommandSizeAdjust(tableName, "end");
        this.sizeAdjustStart = Math.min(adjustStart, adjustEnd);
        this.sizeAdjustEnd = Math.max(adjustStart, adjustEnd);
    }

    public void initialize(CostBenchDbManager dbManager, IsolationLevel isolation, String option, List<TimeRecord> records) {
        this.dbmsType = BenchConst.dbmsType();
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
        if (!BenchConst.timeCommandExecute(tableName, sqlName)) {
            return;
        }

        var txOption = getOption();
        var setting = TgTmSetting.ofAlways(txOption);
        var sizeTitle = getSizeTitle();

        var record = new TimeRecord(dbmsType, dbManager.getName(), isolation, option, tableName, sizeTitle, sqlName);
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

    protected String getSizeTitle() {
        String s = Integer.toString(size);
        if (sizeAdjustStart == 0 && sizeAdjustEnd == 0) {
            return s;
        }
        return String.format("%s[%d:%d]", s, sizeAdjustStart, sizeAdjustEnd);
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
