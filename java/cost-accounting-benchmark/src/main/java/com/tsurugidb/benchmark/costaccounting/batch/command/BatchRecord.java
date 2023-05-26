package com.tsurugidb.benchmark.costaccounting.batch.command;

import java.math.BigDecimal;
import java.math.RoundingMode;

import javax.annotation.Nullable;

import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.benchmark.costaccounting.batch.StringUtil;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class BatchRecord {

    public static String header() {
        return "dbmsType, option, cover rate, scope, label, elapsed[s], tryCount, abortCount, difference, vsz[GB], rss[GB]";
    }

    private final BatchConfig config;
    private final OnlineConfig onlineConfig;
    private final int attempt;
    private final DbmsType dbmsType;
    private long start;
    private long elapsedMillis;
    private int itemCount;
    private int tryCount;
    private int abortCount;
    private String diffCount;
    private long vsz;
    private long rss;

    public BatchRecord(BatchConfig config, @Nullable OnlineConfig onlineConfig, int attempt) {
        this.config = config;
        this.onlineConfig = onlineConfig;
        this.attempt = attempt;
        this.dbmsType = BenchConst.dbmsType();
    }

    public void start() {
        this.start = System.currentTimeMillis();
    }

    public void finish(int itemCount, int tryCount, int abortCount) {
        this.elapsedMillis = System.currentTimeMillis() - start;
        this.itemCount = itemCount;
        this.tryCount = tryCount;
        this.abortCount = abortCount;
    }

    public void setDiff(String diffCount) {
        this.diffCount = diffCount;
    }

    public void setDiff(int diffCount) {
        setDiff(Integer.toString(diffCount));
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

    public String option() {
        if (dbmsType != DbmsType.TSURUGI) {
            return config.getIsolationLevel().name();
        }

        var txOption = config.getDefaultTxOption();
        switch (txOption.type()) {
        case SHORT:
            return "OCC";
        case LONG:
            return "LTX";
        default:
            return txOption.type().name();
        }
    }

    public String executeType() {
        return config.getExecuteType();
    }

    public String scope() {
        String s = executeType();
        switch (s) {
        case BenchConst.SEQUENTIAL_SINGLE_TX:
            return "sequential single-tx";
        case BenchConst.SEQUENTIAL_FACTORY_TX:
            return "sequential tx-per-factory";
        case BenchConst.PARALLEL_SINGLE_TX:
            return "parallel single-tx";
        case BenchConst.PARALLEL_FACTORY_TX:
            return "parallel tx-per-factory";
        case BenchConst.PARALLEL_FACTORY_SESSION:
            return "parallel session-per-factory";
        default:
            return s;
        }
    }

    public String factory() {
        String label = BenchConst.batchCommandLabel();
        if (label != null) {
            return label;
        }
        return StringUtil.toString(config.getFactoryList(), "/");
    }

    private String coverRate() {
        if (this.onlineConfig == null) {
            return "-";
        }
        return Integer.toString(onlineConfig.getCoverRate());
    }

    public String numberOfDifference() {
        return this.diffCount;
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
        sb.append(", option=");
        sb.append(option());
        sb.append(", scope=");
        sb.append(scope());
        sb.append(", factoryCount=");
        sb.append(factory());
        if (this.onlineConfig != null) {
            sb.append(", coverRate=");
            sb.append(onlineConfig.getCoverRate());
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(64);
        sb.append(dbmsType);
        sb.append(",");
        sb.append(option());
        sb.append(",");
        sb.append(coverRate());
        sb.append(",");
        sb.append(scope());
        sb.append(",");
        sb.append(factory());
        sb.append(" ");
        sb.append(itemCount);
        sb.append(",");
        sb.append(elapsedSec());
        sb.append(",");
        sb.append(tryCount);
        sb.append(",");
        sb.append(abortCount);
        sb.append(",");
        sb.append(numberOfDifference());
        sb.append(",");
        sb.append(vsz == -1 ? "-" : String.format("%.1f", vsz / 1024f / 1024f / 1024f));
        sb.append(",");
        sb.append(rss == -1 ? "-" : String.format("%.1f", rss / 1024f / 1024f / 1024f));
        return sb.toString();
    }
}
