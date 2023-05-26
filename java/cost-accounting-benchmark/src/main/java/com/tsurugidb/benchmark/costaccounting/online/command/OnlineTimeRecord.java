package com.tsurugidb.benchmark.costaccounting.online.command;

import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter;
import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;

public class OnlineTimeRecord {

    public static final List<String> HEADER_LIST = List.of( //
            "| title | tx option | cover rate | dedicated time[ms] | numbers of txs | latency<br>avg[ms] | latency<br>min[ms] | latency<br>max[ms] | committed tx through put[task/s] |", //
            "|-------|-----------|-----------:|-------------------:|---------------:|-------------------:|-------------------:|-------------------:|---------------------------------:|");

    private final OnlineConfig config;
    private final String title;
    private final long dedicatedTime;
    private final BenchDbCounter counter;

    public OnlineTimeRecord(OnlineConfig config, String title, long dedicatedTime, BenchDbCounter counter) {
        this.config = config;
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
        sb.append(counter.getTxOptionDescription(title));
        sb.append("|");
        sb.append(config.getCoverRateForReport(title));
        sb.append("|");

        // dedicated time
        sb.append(String.format("%,d", dedicatedTime));
        sb.append("|");

        {
            var time = counter.getTime(title, CounterName.TASK_SUCCESS);

            // numbers of txs
            sb.append(String.format("%,d", time.getCount()));
            sb.append("|");

            // latency
            sb.append(time.getAvgTimeMillisText());
            sb.append("|");
            sb.append(time.getMinTimeMillisText());
            sb.append("|");
            sb.append(time.getMaxTimeMillisText());
            sb.append("|");

            // committed tx through put
            sb.append(String.format("%,.3f", (double) time.getCount() / dedicatedTime * 1000));
            sb.append("|");
        }

        return sb.toString();
    }
}
