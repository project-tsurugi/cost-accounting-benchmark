package com.tsurugidb.benchmark.costaccounting.online.command;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class OnlineResult {

    public static String header() {
        return "dbmsType, online tx, periodic tx, label, elapsed[s], vsz[GB], rss[GB]";
    }

    private final OnlineConfig config;
    private final int attempt;
    private final DbmsType dbmsType;
    private long start;
    private long elapsedMillis;
    private long vsz;
    private long rss;

    public OnlineResult(OnlineConfig config, int attempt) {
        this.config = config;
        this.attempt = attempt;
        this.dbmsType = BenchConst.dbmsType();
    }

    public void start() {
        this.start = System.currentTimeMillis();
    }

    public void finish() {
        this.elapsedMillis = System.currentTimeMillis() - start;
    }

    public void setMemInfo(long vsz, long rss) {
        this.vsz = vsz;
        this.rss = rss;
    }

    public int attempt() {
        return this.attempt;
    }

    public DbmsType dbmsType() {
        return this.dbmsType;
    }

    public String option(String key) {
        if (dbmsType != DbmsType.TSURUGI) {
            return config.getIsolationLevel().name();
        }

        return config.getTxOption(key);
    }

    public long elapsedMillis() {
        return this.elapsedMillis;
    }

    public String elapsedSec() {
        return BigDecimal.valueOf(elapsedMillis).divide(BigDecimal.valueOf(1000), 3, RoundingMode.UNNECESSARY).toPlainString();
    }

    public String getParamString() {
        var sb = new StringBuilder(64);
        sb.append("dbmsType=");
        sb.append(dbmsType);
        sb.append(", onlineTx=");
        sb.append(option("online"));
        sb.append(", periodicTx=");
        sb.append(option("periodic"));
        return sb.toString();
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(64);
        sb.append(dbmsType);
        sb.append(",");
        sb.append(option("online"));
        sb.append(",");
        sb.append(option("periodic"));
        sb.append(",");
        sb.append(config.getLabel());
        sb.append(",");
        sb.append(elapsedSec());
        sb.append(",");
        sb.append(vsz == -1 ? "-" : String.format("%.1f", vsz / 1024f / 1024f / 1024f));
        sb.append(",");
        sb.append(rss == -1 ? "-" : String.format("%.1f", rss / 1024f / 1024f / 1024f));
        return sb.toString();
    }
}
