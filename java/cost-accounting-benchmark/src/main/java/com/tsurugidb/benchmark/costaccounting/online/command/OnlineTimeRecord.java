package com.tsurugidb.benchmark.costaccounting.online.command;

import java.util.List;
import java.util.function.ToDoubleFunction;
import java.util.regex.Pattern;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter;
import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;

public class OnlineTimeRecord {

    public static final List<String> HEADER_LIST = List.of( //
            "| title | tx option | cover rate | dedicated time[ms] | numbers of txs | latency<br>avg[ms] | latency<br>min[ms] | latency<br>max[ms] | committed tx through put[task/s] |", //
            "|-------|-----------|-----------:|-------------------:|---------------:|-------------------:|-------------------:|-------------------:|---------------------------------:|");

    public static OnlineTimeRecord of(OnlineConfig config, String taskName, long dedicatedTime, BenchDbCounter counter, OnlineTimeRecord compareBaseRecord) {
        var record = new OnlineTimeRecord();
        record.taskName = taskName;
        record.txOption = counter.getTxOptionDescription(taskName);
        record.coverRate = config.getCoverRateForReport(taskName);
        record.dedicatedTime = dedicatedTime;
        var time = counter.getTime(taskName, CounterName.TASK_SUCCESS);
        record.numbersOfTxs = time.getCount();
        record.latencyAvg = time.getAvgTimeMillis();
        record.latencyMin = time.getMinTimeMillis();
        record.latencyMax = time.getMaxTimeMillis();
        record.committedTxThroughPut = (double) time.getCount() / dedicatedTime * 1000;
        record.compareBaseRecord = compareBaseRecord;
        return record;
    }

    public static OnlineTimeRecord parse(String line) {
        if (line.startsWith("|") && line.endsWith("|")) {
            String[] header = split(HEADER_LIST.get(0));
            String[] ss = split(line);
            if (ss.length == header.length) {
                if (ss[0].trim().equals("title") || ss[0].chars().allMatch(c -> c == '-')) {
                    return null;
                }

                var record = new OnlineTimeRecord();
                try {
                    int i = 0;
                    record.taskName = ss[i++];
                    record.txOption = ss[i++];
                    record.coverRate = ss[i++];
                    record.dedicatedTime = parseLong(ss[i++]);
                    record.numbersOfTxs = parseInt(ss[i++]);
                    record.latencyAvg = parseDouble(ss[i++]);
                    record.latencyMin = parseDouble(ss[i++]);
                    record.latencyMax = parseDouble(ss[i++]);
                    record.committedTxThroughPut = parseDouble(ss[i++]);
                } catch (Exception e) {
                    throw new RuntimeException("parse error. line=" + line, e);
                }
                return record;
            }
        }
        return null;
    }

    private static String[] split(String s) {
        String s1 = s.substring(1, s.length() - 2);
        return s1.split(Pattern.quote("|"));
    }

    private static int parseInt(String s) {
        return Integer.parseInt(s.replace(",", ""));
    }

    private static long parseLong(String s) {
        return Long.parseLong(s.replace(",", ""));
    }

    private static double parseDouble(String s) {
        int n = s.indexOf('(');
        if (n >= 0) {
            s = s.substring(0, n).trim();
        }

        if (s.equals("-")) {
            return Double.NaN;
        }
        return Double.parseDouble(s.replace(",", ""));
    }

    String taskName;
    String txOption;
    String coverRate;
    long dedicatedTime;
    int numbersOfTxs;
    double latencyAvg;
    double latencyMin;
    double latencyMax;
    double committedTxThroughPut;
    OnlineTimeRecord compareBaseRecord;

    public String taskName() {
        return this.taskName;
    }

    public double latencyAvg() {
        return this.latencyAvg;
    }

    public double latencyMin() {
        return this.latencyMin;
    }

    public double latencyMax() {
        return this.latencyMax;
    }

    public double committedTxThroughPut() {
        return this.committedTxThroughPut;
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(128);
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
            sb.append(String.format("%,d", numbersOfTxs));

            // latency
            sb.append("|");
            sb.append(toString(OnlineTimeRecord::latencyAvg));
            sb.append("|");
            sb.append(toString(OnlineTimeRecord::latencyMin));
            sb.append("|");
            sb.append(toString(OnlineTimeRecord::latencyMax));

            // committed tx through put
            sb.append("|");
            sb.append(toString(OnlineTimeRecord::committedTxThroughPut));
        }

        sb.append("|");
        return sb.toString();
    }

    private String toString(ToDoubleFunction<OnlineTimeRecord> getter) {
        double value = getter.applyAsDouble(this);
        if (Double.isNaN(value)) {
            return "-";
        }

        String s = String.format("%,.3f", value);
        if (this.compareBaseRecord != null) {
            double base = getter.applyAsDouble(compareBaseRecord);
            if (Double.isNaN(base) || base == 0) {
                return s + "<br>(-%)";
            }
            return s + "<br>(" + String.format("%,.2f", value / base * 100) + "%)";
        }
        return s;
    }
}
