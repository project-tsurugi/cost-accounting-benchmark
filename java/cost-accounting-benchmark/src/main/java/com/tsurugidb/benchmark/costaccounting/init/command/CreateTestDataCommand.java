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
package com.tsurugidb.benchmark.costaccounting.init.command;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.init.InitialData01MeasurementMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData02FactoryMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData03ItemMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData04ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData05CostMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData06StockHistory;
import com.tsurugidb.benchmark.costaccounting.init.InitialData07ResultTable;

public class CreateTestDataCommand implements ExecutableCommand {

    @Override
    public String getDescription() {
        return "Create test data to database.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        InitialData01MeasurementMaster.main();
        InitialData02FactoryMaster.main();
        InitialData03ItemMaster.main();
        InitialData04ItemManufacturingMaster.main();
        InitialData05CostMaster.main();
        InitialData06StockHistory.main();
        InitialData07ResultTable.main();
        return 0;
    }
}
