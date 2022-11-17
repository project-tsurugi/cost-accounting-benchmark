package com.tsurugidb.benchmark.costaccounting.init.command;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.init.ConstraintCheck;

public class CheckConstraintCommand implements ExecutableCommand {

    @Override
    public String getDescription() {
        return "Check constraint of tables.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        ConstraintCheck.main(new String[] {});
        return 0;
    }
}
