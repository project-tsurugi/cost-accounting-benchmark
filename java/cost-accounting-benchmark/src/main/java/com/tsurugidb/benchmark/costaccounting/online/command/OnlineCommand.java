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
