package com.tsurugidb.benchmark.costaccounting.online.command;

import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter;
import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;

public class OnlineTimeRecord {

    public static final List<String> HEADER_LIST = List.of( //
            "| title | dedicated time[ms] | numbers of txs | latency<br>avg[ms] | latency<br>min[ms] | latency<br>max[ms] | committed tx through put |", //
            "|-------|-------------------:|---------------:|-------------------:|-------------------:|-------------------:|-------------------------:|");

    private final String title;
    private final long dedicatedTime;
    private final BenchDbCounter counter;

    public OnlineTimeRecord(String title, long dedicatedTime, BenchDbCounter counter) {
        this.title = title;
        this.dedicatedTime = dedicatedTime;
        this.counter = counter;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(64);
        sb.append("|");
        sb.append(title);
        sb.append("|");

        // dedicated time
        sb.append(dedicatedTime);
        sb.append("|");

        // numbers of txs
        sb.append(counter.getCount(title, CounterName.TASK_START));
        sb.append("|");

        {
            // latency
            var time = counter.getTime(title, CounterName.TASK_SUCCESS);
            sb.append(String.format("%.3f", time.getAvgTimeMillis()));
            sb.append("|");
            sb.append(time.getMinTimeMillisText());
            sb.append("|");
            sb.append(time.getMaxTimeMillisText());
            sb.append("|");

            // committed tx through put
            sb.append(time.getCount());
            sb.append("|");
        }

        return sb.toString();
    }
}
