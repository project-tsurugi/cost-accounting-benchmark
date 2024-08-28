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
package com.tsurugidb.benchmark.costaccounting.init;

import java.util.concurrent.atomic.AtomicInteger;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.FactoryMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class InitialData02FactoryMaster extends InitialData {

    public static void main(String... args) throws Exception {
        int factorySize = BenchConst.initFactorySize();
        new InitialData02FactoryMaster().main(factorySize);
    }

    public InitialData02FactoryMaster() {
        super(null);
    }

    private void main(int size) {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            insertFactoryMaster(size);
        } finally {
            shutdown();
        }

        logEnd();
    }

    private void insertFactoryMaster(int size) {
        FactoryMasterDao dao = dbManager.getFactoryMasterDao();

        var setting = getSetting(FactoryMasterDao.TABLE_NAME);
        var insertCount = new AtomicInteger();
        dbManager.execute(setting, () -> {
            dao.truncate();
            insertCount.set(0);
            insertFactoryMaster(size, dao, insertCount);
        });
        LOG.info("insert {}={}", FactoryMasterDao.TABLE_NAME, insertCount.get());
    }

    private void insertFactoryMaster(int size, FactoryMasterDao dao, AtomicInteger insertCount) {
        for (int i = 0; i < size; i++) {
            int fId = i + 1;

            FactoryMaster entity = new FactoryMaster();
            entity.setFId(fId);
            entity.setFName("Factory" + fId);

            dao.insert(entity);
            insertCount.incrementAndGet();
        }
    }
}
