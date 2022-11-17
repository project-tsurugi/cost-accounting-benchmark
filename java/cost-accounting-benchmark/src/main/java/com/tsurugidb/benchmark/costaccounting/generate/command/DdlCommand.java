package com.tsurugidb.benchmark.costaccounting.generate.command;

import java.nio.file.Path;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.DdlGenerator;

public class DdlCommand implements ExecutableCommand {

    @Override
    public String getDescription() {
        return "Generate ddl file. arg1={oracle|postgresql|tsurugi}, arg2=<output ddl file path>";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        if (args.length <= 2) {
            System.err.println("ERROR: too few arguments.");
            System.err.println(getDescription());
            return 1;
        }

        var dbmsType = DbmsType.of(args[1]);
        var generator = DdlGenerator.createDdlGenerator(dbmsType);
        var ddlFilePath = Path.of(args[2]);
        generator.writeDdlFile(ddlFilePath);
        return 0;
    }
}
