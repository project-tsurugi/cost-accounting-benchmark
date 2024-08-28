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

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;

public class InitialData07ResultTable extends InitialData {

    public static void main(String... args) throws Exception {
        new InitialData07ResultTable().main();
    }

    public InitialData07ResultTable() {
        super(null);
    }

    private void main() {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            deleteResultTable();
        } finally {
            shutdown();
        }

        logEnd();
    }

    private void deleteResultTable() {
        ResultTableDao dao = dbManager.getResultTableDao();

        var setting = getSetting(ResultTableDao.TABLE_NAME);
        dbManager.execute(setting, () -> {
            dao.truncate();
        });
    }
}
