package com.tsurugidb.benchmark.costaccounting.batch.command;

import com.tsurugidb.benchmark.costaccounting.batch.BatchConfig;
import com.tsurugidb.benchmark.costaccounting.batch.StringUtil;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class BatchRecord {

    public static String header() {
        return "dbmsType, option, scope, factory, elapsedMills, tryCount, abortCount, diffrence";
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
        return config.getExecuteType();
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
        sb.append(elapsedMillis);
        sb.append(",");
        sb.append(tryCount);
        sb.append(",");
        sb.append(abortCount);
        sb.append(",");
        sb.append(numberOfDiffrence());
        return sb.toString();
    }
}
