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
