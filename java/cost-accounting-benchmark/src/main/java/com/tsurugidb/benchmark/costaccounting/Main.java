package com.tsurugidb.benchmark.costaccounting;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.costaccounting.batch.command.BatchCommand;
import com.tsurugidb.benchmark.costaccounting.debug.DebugCommand;
import com.tsurugidb.benchmark.costaccounting.debug.TestCommand;
import com.tsurugidb.benchmark.costaccounting.generate.command.DdlCommand;
import com.tsurugidb.benchmark.costaccounting.init.command.CheckConstraintCommand;
import com.tsurugidb.benchmark.costaccounting.init.command.CreateTableCommand;
import com.tsurugidb.benchmark.costaccounting.init.command.CreateTestDataCommand;
import com.tsurugidb.benchmark.costaccounting.init.command.DumpCsvCommand;
import com.tsurugidb.benchmark.costaccounting.online.command.OnlineCommand;
import com.tsurugidb.benchmark.costaccounting.time.TimeCommand;

public class Main {

    private static final Map<String, Supplier<ExecutableCommand>> COMMAND_MAP = new LinkedHashMap<>();
    static {
        addCommand("generateDdl", DdlCommand::new);
        addCommand("createTable", CreateTableCommand::new);
        addCommand("createTestData", CreateTestDataCommand::new);
        addCommand("checkConstraint", CheckConstraintCommand::new);
        addCommand("dumpCsv", DumpCsvCommand::new);
        addCommand("time", TimeCommand::new);
        addCommand("executeBatch", BatchCommand::new);
        addCommand("executeOnline", OnlineCommand::new);
        addCommand("debug", DebugCommand::new);
        addCommand("test", TestCommand::new);
    }

    private static void addCommand(String name, Supplier<ExecutableCommand> creator) {
        COMMAND_MAP.put(name, creator);
    }

    public static void main(String... args) throws Exception {
        if (args.length < 1) {
            System.err.println("ERROR: No argument is specified.");
            usage();
            System.exit(1);
        }

        String name = args[0];
        var creator = COMMAND_MAP.get(name);
        if (creator == null) {
            System.err.printf("ERROR: Command '%s' is not available.%n", name);
            usage();
            System.exit(1);
        }

        var command = creator.get();
        int exitCode = command.executeCommand(args);
        System.exit(exitCode);
    }

    private static void usage() {
        System.err.println();
        System.err.println("usage: <command> [<option>...]");
        for (var entry : COMMAND_MAP.entrySet()) {
            String name = entry.getKey();
            String description = entry.getValue().get().getDescription();
            System.err.printf("  %s: %s%n", name, description);
        }
    }
}
