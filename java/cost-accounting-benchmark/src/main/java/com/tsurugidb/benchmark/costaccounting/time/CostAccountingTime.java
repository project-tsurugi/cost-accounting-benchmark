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
package com.tsurugidb.benchmark.costaccounting.time;

import java.io.IOException;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager.DbManagerPurpose;
import com.tsurugidb.benchmark.costaccounting.time.table.CostMasterTime;
import com.tsurugidb.benchmark.costaccounting.time.table.ItemMasterTime;
import com.tsurugidb.benchmark.costaccounting.time.table.ResultTableTime;
import com.tsurugidb.benchmark.costaccounting.time.table.TableTime;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;

public class CostAccountingTime {

    private final IsolationLevel isolation;
    private final String txOption;

    public CostAccountingTime(IsolationLevel isolation, String txOption) {
        this.isolation = isolation;
        this.txOption = txOption;
    }

    public int main(List<TimeRecord> records) throws IOException {
        TableTime[] list = { //
                new ItemMasterTime(), //
                new CostMasterTime(), //
                new ResultTableTime(), //
        };

        int exitCode = 0;
        try (CostBenchDbManager manager = createDbManager()) {
            for (var time : list) {
                time.initialize(manager, isolation, txOption, records);
                time.execute();
            }
        }
        return exitCode;
    }

    private CostBenchDbManager createDbManager() {
        var type = BenchConst.timeCommandDbManagerType();
        return CostBenchDbManager.createInstance(type, DbManagerPurpose.TIME, isolation, false);
    }
}
