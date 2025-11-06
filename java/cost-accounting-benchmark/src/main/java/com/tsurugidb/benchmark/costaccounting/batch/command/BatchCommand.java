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
