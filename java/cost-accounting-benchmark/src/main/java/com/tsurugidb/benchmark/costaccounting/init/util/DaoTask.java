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
