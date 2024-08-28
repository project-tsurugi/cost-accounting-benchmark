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
package com.tsurugidb.benchmark.costaccounting.init.util;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.RecursiveAction;
import java.util.concurrent.atomic.AtomicInteger;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

@SuppressWarnings("serial")
public abstract class DaoListTask<T> extends RecursiveAction {

    private final CostBenchDbManager dbManager;
    private final TgTmSetting setting;

    private final List<T> list = new ArrayList<>();
    private final AtomicInteger insertCount = new AtomicInteger();

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
            insertCount.set(0);
            startTransaction();
            for (T t : list) {
                execute(t, insertCount);
            }
        });
    }

    protected void startTransaction() {
        // do override
    }

    protected abstract void execute(T t, AtomicInteger insertCount);

    public int getInsertCount() {
        return insertCount.get();
    }
}
