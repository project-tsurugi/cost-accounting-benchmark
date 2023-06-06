package com.tsurugidb.benchmark.costaccounting.online.command;

import java.util.Objects;
import java.util.regex.Pattern;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;

public class OnlineAppReportHeader {

    public static OnlineAppReportHeader ofOnline(DbmsType dbmsType, String label, String onlineTxOption, String periodicTxOption, int coverRate) {
        String title = dbmsType.name() + " " + label + " online{" + onlineTxOption + ":" + periodicTxOption + " coverRate=" + coverRate + "}";
        return new OnlineAppReportHeader(title, onlineTxOption, periodicTxOption, coverRate);
    }

    private static final Pattern ONLINE_PATTERN = Pattern.compile("(?<dbmsType>.+) (?<label>.+) online\\{(?<onlineTxOption>.+):(?<periodicTxOption>.+) coverRate=(?<coverRate>\\d+)\\}");

    public static OnlineAppReportHeader ofBatch(DbmsType dbmsType, String label, String scope, String batchTxOption, OnlineConfig onlineConfig) {
        String onlineTxOption = onlineConfig.getTxOption("online");
        String periodicTxOption = onlineConfig.getTxOption("periodic");
        int coverRate = onlineConfig.getCoverRate();
        String title = dbmsType.name() + " " + label + " batch{" + scope + " " + batchTxOption + "} online{" + onlineTxOption + ":" + periodicTxOption + " coverRate=" + coverRate + "}";
        return new OnlineAppReportHeader(title, onlineTxOption, periodicTxOption, coverRate);
    }

    private static final Pattern BATCH_PATTERN = Pattern
            .compile("(?<dbmsType>.+) (?<label>.+) batch\\{(?<scope>.+) (?<batchTxOption>.+)\\} online\\{(?<onlineTxOption>.+):(?<periodicTxOption>.+) coverRate=(?<coverRate>\\d+)\\}");

    public static OnlineAppReportHeader parse(String line) {
        {
            var m = ONLINE_PATTERN.matcher(line);
            if (m.matches()) {
                String onlineTxOption = m.group("onlineTxOption");
                String periodicTxOption = m.group("periodicTxOption");
                int coverRate = Integer.parseInt(m.group("coverRate"));
                return new OnlineAppReportHeader(line, onlineTxOption, periodicTxOption, coverRate);
            }
        }
        {
            var m = BATCH_PATTERN.matcher(line);
            if (m.matches()) {
                String onlineTxOption = m.group("onlineTxOption");
                String periodicTxOption = m.group("periodicTxOption");
                int coverRate = Integer.parseInt(m.group("coverRate"));
                return new OnlineAppReportHeader(line, onlineTxOption, periodicTxOption, coverRate);
            }
        }
        return null;
    }

    private final String title;
    private final String onlineTxOption;
    private final String periodicTxOption;
    private final int coverRate;

    private OnlineAppReportHeader(String title, String onlineTxOption, String periodicTxOption, int coverRate) {
        this.title = title;
        this.onlineTxOption = onlineTxOption;
        this.periodicTxOption = periodicTxOption;
        this.coverRate = coverRate;
    }

    public String onlineTxOption() {
        return this.onlineTxOption;
    }

    public String periodicTxOption() {
        return this.periodicTxOption;
    }

    public int coverRate() {
        return this.coverRate;
    }

    @Override
    public int hashCode() {
        return Objects.hash(title, onlineTxOption, coverRate);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        OnlineAppReportHeader other = (OnlineAppReportHeader) obj;
        return Objects.equals(title, other.title) && Objects.equals(onlineTxOption, other.onlineTxOption) && coverRate == other.coverRate;
    }

    @Override
    public String toString() {
        return this.title;
    }
}
