package com.tsurugidb.benchmark.costaccounting.batch.command;

import java.math.BigDecimal;
import java.math.RoundingMode;

import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.benchmark.costaccounting.batch.StringUtil;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class BatchRecord {

    public static String header() {
        return "dbmsType, option, scope, factory, elapsed[s], tryCount, abortCount, diffrence";
    }

    private final BatchConfig config;
    private final DbmsType dbmsType;
    private long start;
    private long elapsedMillis;
    private int tryCount;
    private int abortCount;

    public BatchRecord(BatchConfig config) {
        this.config = config;
        this.dbmsType = BenchConst.dbmsType();
    }

    public void start() {
        this.start = System.currentTimeMillis();
    }

    public void finish(int tryCount, int abortCount) {
        this.elapsedMillis = System.currentTimeMillis() - start;
        this.tryCount = tryCount;
        this.abortCount = abortCount;
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

    public String scope() {
        String s = config.getExecuteType();
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
        return StringUtil.toString(config.getFactoryList(), "/");
    }

    public String numberOfDiffrence() {
        return "-";
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
        return sb.toString();
    }

    @Override
    public String toString() {
        var sb = new StringBuilder(64);
        sb.append(dbmsType);
        sb.append(",");
        sb.append(option());
        sb.append(",");
        sb.append(scope());
        sb.append(",");
        sb.append(factory());
        sb.append(",");
        sb.append(elapsedSec());
        sb.append(",");
        sb.append(tryCount);
        sb.append(",");
        sb.append(abortCount);
        sb.append(",");
        sb.append(numberOfDiffrence());
        return sb.toString();
    }
}
