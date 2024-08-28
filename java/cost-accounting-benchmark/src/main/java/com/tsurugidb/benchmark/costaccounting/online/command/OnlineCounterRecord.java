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
package com.tsurugidb.benchmark.costaccounting.online.command;

import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter;
import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;

public class OnlineCounterRecord {

    public static final List<String> HEADER_LIST = List.of( //
            "| title | tx option | cover rate | threads | tpm/thread | success | occ-try | occ-abort | occ-success | occ<br>abandoned<br>retry | ltx-try | ltx-abort | ltx-success | ltx<br>abandoned<br>retry | fail | task start | target nothing | task success | task fail |", //
            "|-------|-----------|-----------:|--------:|-----------:|--------:|--------:|----------:|------------:|--------------------------:|--------:|----------:|------------:|--------------------------:|-----:|-----------:|---------------:|-------------:|----------:|");

    public static final String FOOTER = "※occ-abortの括弧内は、ERR_CONFLICT_ON_WRITE_PRESERVEの件数; CC_OCC_WP_VERIFYの件数";

    private final OnlineConfig config;
    private final String title;
    private final String tpm;
    private final BenchDbCounter counter;

    public OnlineCounterRecord(OnlineConfig config, String title, String tpm, BenchDbCounter counter) {
        this.config = config;
        this.title = title;
        this.tpm = tpm;
        this.counter = counter;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(64);
        sb.append("|");
        sb.append(title);
        sb.append("|");
        sb.append(counter.getTxOptionDescription(title));
        sb.append("|");
        sb.append(config.getCoverRateForReport(title));
        sb.append("|");
        sb.append(config.getThreadSize(title));
        sb.append("|");
        sb.append(tpm);
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.SUCCESS));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.OCC_TRY));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.OCC_ABORT));
        sb.append("(");
        sb.append(counter.getCount(title, CounterName.CONFLIT_ON_WP));
        sb.append(";");
        sb.append(counter.getCount(title, CounterName.OCC_WP_VERIFY));
        sb.append(")");
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.OCC_SUCCESS));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.OCC_ABANDONED_RETRY));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.LTX_TRY));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.LTX_ABORT));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.LTX_SUCCESS));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.LTX_ABANDONED_RETRY));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.FAIL));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.TASK_START));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.TASK_NOTHING));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.TASK_SUCCESS));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.TASK_FAIL));
        sb.append("|");
        return sb.toString();
    }
}
