/*
 * Copyright 2023-2025 Project Tsurugi.
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
