package com.tsurugidb.benchmark.costaccounting.online.command;

import java.util.Arrays;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.online.CostAccountingOnline;

public class OnlineCommand implements ExecutableCommand {

    @Override
    public String getDescription() {
        return "Execute online. [arg1=<date>]. Ctrl-C to stop.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        String[] onlineArgs = Arrays.copyOfRange(args, 1, args.length);
        return CostAccountingOnline.main0(onlineArgs);
    }
}
