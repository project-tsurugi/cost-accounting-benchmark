package com.tsurugidb.benchmark.costaccounting.batch.command;

import java.util.Arrays;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.batch.CostAccountingBatch;

public class BatchCommand implements ExecutableCommand {

    @Override
    public String getDescription() {
        return "Execute batch. [arg1=<date>], [arg2=<factory list>], [arg3=<commit ratio>]";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        String[] batchArgs = Arrays.copyOfRange(args, 1, args.length);
        return CostAccountingBatch.main0(batchArgs);
    }
}
