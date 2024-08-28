/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.benchmark.costaccounting.time;

import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.ExecutableCommand;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class TimeCommand implements ExecutableCommand {
    private static final Logger LOG = LoggerFactory.getLogger(TimeCommand.class);

    @Override
    public String getDescription() {
        return "Measurement time.";
    }

    @Override
    public int executeCommand(String... args) throws Exception {
        var outputPath = Path.of(BenchConst.timeCommandResultFile());
        var outputDir = outputPath.getParent();
        if (outputDir != null && !Files.exists(outputDir)) {
            Files.createDirectories(outputDir);
        }

        var isolationList = BenchConst.timeCommandIsolationLevel();
        LOG.info("isolationList={}", isolationList);
        var txList = BenchConst.timeCommandTxOption();
        LOG.info("txList={}", txList);

        int exitCode = 0;
        var records = new ArrayList<TimeRecord>();
        for (var isolationLevel : isolationList) {
            for (var txOption : txList) {
                var time = new CostAccountingTime(isolationLevel, txOption);
                exitCode |= time.main(records);

                writeResult(outputPath, records);
            }
        }
        return exitCode;
    }

    private void writeResult(Path outputPath, List<TimeRecord> records) throws IOException {
        try (var pw = new PrintWriter(Files.newBufferedWriter(outputPath))) {
            pw.println(TimeRecord.header());
            for (var record : records) {
                pw.println(record);
            }
        }
    }
}
