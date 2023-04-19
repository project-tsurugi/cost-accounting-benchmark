package com.tsurugidb.benchmark.costaccounting.debug;

import java.util.Arrays;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.online.periodic.BenchPeriodicUpdateStockTask;

public class TestCommand implements ExecutableCommand {

    @Override
    public String getDescription() {
        return "Execute test. args={command}";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        if (args.length <= 1) {
            System.err.println("ERROR: too few arguments.");
            System.err.println(getDescription());
            return 1;
        }

        String type = args[1];
        switch (type) {
        case "update-stock":
            BenchPeriodicUpdateStockTask.main(Arrays.copyOfRange(args, 2, args.length));
            break;
        default:
            throw new UnsupportedOperationException("unsupported operation. type=" + type);
        }

        return 0;
    }
}
