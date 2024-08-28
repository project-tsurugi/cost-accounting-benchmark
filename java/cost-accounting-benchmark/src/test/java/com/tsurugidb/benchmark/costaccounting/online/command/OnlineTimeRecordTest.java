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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDate;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter;
import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;

class OnlineTimeRecordTest {

    @Test
    void parseHeader() {
        for (String line : OnlineTimeRecord.HEADER_LIST) {
            assertNull(OnlineTimeRecord.parse(line));
        }
    }

    @Test
    void parse() {
        String taskName = "testTask";
        int coverRate = 75;
        long dedicatedTime = 10_000;
        String txOption = "OCC*3";
        var timeList = List.of(1_000_000L, 2_000_000L, 3_000_000L);
        var record = create(taskName, coverRate, dedicatedTime, txOption, timeList, null);

        assertEquals(taskName, record.taskName());
        assertEquals(txOption, record.txOption);
        assertEquals(Integer.toString(coverRate), record.coverRate);
        assertEquals(dedicatedTime, record.dedicatedTime);
        assertEquals(timeList.size(), record.numbersOfTxs);
        assertEquals(timeList.stream().mapToLong(Long::longValue).average().getAsDouble() / 1000_000d, record.latencyAvg());
        assertEquals(timeList.stream().mapToLong(Long::longValue).max().getAsLong() / 1000_000d, record.latencyMax());
        assertEquals(timeList.stream().mapToLong(Long::longValue).min().getAsLong() / 1000_000d, record.latencyMin());
        assertEquals((double) timeList.size() / dedicatedTime * 1000, record.committedTxThroughPut());

        // parse test
        String line = record.toString();
        var parse = OnlineTimeRecord.parse(line);
        assertEquals(record.taskName(), parse.taskName());
        assertEquals(record.txOption, parse.txOption);
        assertEquals(record.coverRate, parse.coverRate);
        assertEquals(record.dedicatedTime, parse.dedicatedTime);
        assertEquals(record.numbersOfTxs, parse.numbersOfTxs);
        assertEquals(record.latencyAvg(), parse.latencyAvg());
        assertEquals(record.latencyMin(), parse.latencyMin());
        assertEquals(record.latencyMax(), parse.latencyMax());
        assertEquals(record.committedTxThroughPut(), parse.committedTxThroughPut());
    }

    @Test
    void compareBase() {
        String taskName = "testTask";
        int coverRate = 75;
        long dedicatedTime = 10_000;
        String txOption = "OCC*3";
        var timeList = List.of(1_000_000L, 2_000_000L, 3_000_000L);
        OnlineTimeRecord compareBaseRecord = create(taskName, coverRate, dedicatedTime + 1, txOption, List.of(10_000_000L, 20_000_000L, 30_000_000L), null);
        var record = create(taskName, coverRate, dedicatedTime, txOption, timeList, compareBaseRecord);

        var sb = new StringBuilder();
        sb.append("|");
        sb.append(taskName);
        sb.append("|");
        sb.append(txOption);
        sb.append("|");
        sb.append(coverRate);

        // dedicated time
        sb.append("|");
        sb.append(String.format("%,d", dedicatedTime));

        {
            // numbers of txs
            sb.append("|");
            sb.append(String.format("%,d", timeList.size()));

            // latency
            sb.append("|");
            sb.append(toString(record.latencyAvg(), compareBaseRecord.latencyAvg()));
            sb.append("|");
            sb.append(toString(record.latencyMin(), compareBaseRecord.latencyMin()));
            sb.append("|");
            sb.append(toString(record.latencyMax(), compareBaseRecord.latencyMax()));

            // committed tx through put
            sb.append("|");
            sb.append(toString(record.committedTxThroughPut(), compareBaseRecord.committedTxThroughPut()));
        }

        sb.append("|");
        assertEquals(sb.toString(), record.toString());
    }

    private String toString(double value, double base) {
        return String.format("%,.3f<br>(%,.2f%%)", value, value / base * 100);
    }

    private static OnlineTimeRecord create(String taskName, int coverRate, long dedicatedTime, String txOption, List<Long> timeList, OnlineTimeRecord compareBaseRecord) {
        var config = new OnlineConfig(LocalDate.of(2023, 6, 6));
        config.setCoverRate(coverRate);
        config.getCoverRateForTask(taskName);
        var counter = new BenchDbCounter();
        counter.setTxOptionDescription(taskName, txOption);
        for (long time : timeList) {
            counter.addTime(taskName, CounterName.TASK_SUCCESS, time);
        }
        return OnlineTimeRecord.of(config, taskName, dedicatedTime, counter, compareBaseRecord);
    }
}
