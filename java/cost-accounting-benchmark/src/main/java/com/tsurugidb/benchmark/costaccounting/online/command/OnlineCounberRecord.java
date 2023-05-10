package com.tsurugidb.benchmark.costaccounting.online.command;

import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter;
import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;

public class OnlineCounberRecord {

    public static final List<String> HEADER_LIST = List.of( //
            "| title | tx option | threads | tpm/thread | success | occ-try | occ-abort | occ-success | occ<br>abandoned<br>retry | ltx-try | ltx-abort | ltx-success | ltx<br>abandoned<br>retry | fail | task start | target nothing | task success | task fail |", //
            "|-------|-----------|--------:|-----------:|--------:|--------:|----------:|------------:|--------------------------:|--------:|----------:|------------:|--------------------------:|-----:|-----------:|---------------:|-------------:|----------:|");

    private final String title;
    private final int threads;
    private final String tpm;
    private final BenchDbCounter counter;

    public OnlineCounberRecord(String title, int threads, String tpm, BenchDbCounter counter) {
        this.title = title;
        this.threads = threads;
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
        sb.append(threads);
        sb.append("|");
        sb.append(tpm);
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.SUCCESS));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.OCC_TRY));
        sb.append("|");
        sb.append(counter.getCount(title, CounterName.OCC_ABORT));
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
