package com.tsurugidb.benchmark.costaccounting.init.command;

import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.init.DumpCsv;

public class DumpCsvCommand implements ExecutableCommand {

    @Override
    public String getDescription() {
        return "Dump table to csv file. arg1=<output dir>, [arg2=<table name>]";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        if (args.length <= 1) {
            System.err.println("ERROR: too few arguments.");
            System.err.println(getDescription());
            return 1;
        }
        var outputDir = Path.of(args[1]);
        var list = List.of(Arrays.copyOfRange(args, 2, args.length));

        new DumpCsv().main(outputDir, list);
        return 0;
    }
}
