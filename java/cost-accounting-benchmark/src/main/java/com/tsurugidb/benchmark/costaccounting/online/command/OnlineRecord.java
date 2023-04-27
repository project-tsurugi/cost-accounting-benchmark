package com.tsurugidb.benchmark.costaccounting.online.command;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter;
import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;

public class OnlineRecord {

    public static String header1() {
        return "| title | threads | tpm/thread | success | occ-try | occ-abort | occ-success | occ<br>abandoned<br>retry | ltx-try | ltx-abort | ltx-success | ltx<br>abandoned<br>retry | fail | task start | target nothing | task success | task fail |";
    }

    public static String header2() {
        return "|-------|--------:|-----------:|--------:|--------:|----------:|------------:|--------------------------:|--------:|----------:|------------:|--------------------------:|-----:|-----------:|---------------:|-------------:|----------:|";
    }

    private final String title;
    private final int threads;
    private final String tpm;
    private final BenchDbCounter counter;

    public OnlineRecord(String title, int threads, String tpm, BenchDbCounter counter) {
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
