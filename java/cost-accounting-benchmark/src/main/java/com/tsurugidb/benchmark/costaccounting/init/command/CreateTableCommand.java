package com.tsurugidb.benchmark.costaccounting.init.command;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.init.InitialData00CreateTable;

public class CreateTableCommand implements ExecutableCommand {

    @Override
    public String getDescription() {
        return "Create tables.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        InitialData00CreateTable.main();
        return 0;
    }
}
