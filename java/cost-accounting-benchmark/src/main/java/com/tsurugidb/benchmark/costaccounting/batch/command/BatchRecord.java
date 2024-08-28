/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
        return "dbmsType, option, cover rate, scope, label, elapsed[s], elapsed rate[%], tryCount, abortCount, difference, vsz[GB], rss[GB]";
    }

    public static BatchRecordPart parse(String line) {
        String[] ss = line.split(",");
        if (ss.length != header().split(",").length) {
            return null;
        }
        if (ss[0].contains("dbmsType")) {
            return null;
        }

        var record = new BatchRecordPart(line);
        try {
            int i = 0;
            record.dbmsType = ss[i++].trim();
            record.option = ss[i++].trim();
            record.onlineCoverRate = ss[i++].trim();
            record.scope = ss[i++].trim();
            record.label = ss[i++].trim();
            record.elapsed = Double.parseDouble(ss[i++].trim());
            i++; // elapsed rate
            i++; // tryCount
            i++; // abortCount
            i++; // difference
            record.vsz = ss[i++].trim();
            record.rss = ss[i++].trim();
        } catch (Exception e) {
            throw new RuntimeException("parse error. line=" + line, e);
        }
        return record;
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
    private BatchRecordPart compareBaseRecord;

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

    public int threadSize() {
        return config.getThreadSize();
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
            return "seq-1tx"; // sequential single-tx
        case BenchConst.SEQUENTIAL_FACTORY_TX:
            return "seq-tx/F"; // sequential tx-per-factory
        case BenchConst.PARALLEL_SINGLE_TX:
            return "p" + threadSize() + "-1tx"; // parallel single-tx
        case BenchConst.PARALLEL_FACTORY_TX:
            return "p" + threadSize() + "-tx/F"; // parallel tx-per-factory
        case BenchConst.PARALLEL_FACTORY_SESSION:
            return "p" + threadSize() + "-S/F"; // parallel session-per-factory
        default:
            return s;
        }
    }

    public String label() {
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

    public String elapsedRate() {
        if (this.compareBaseRecord == null) {
            return "-";
        }

        double base = compareBaseRecord.getElapsed();
        if (base == 0) {
            return "-";
        }
        double value = elapsedMillis / 1000d;
        return String.format("%.2f", value / base * 100);
    }

    public void setCompareBaseRecord(BatchRecordPart compareBaseRecord) {
        this.compareBaseRecord = compareBaseRecord;
    }

    public String getParamString() {
        var sb = new StringBuilder(64);
        sb.append("dbmsType=");
        sb.append(dbmsType);
        sb.append(", option=");
        sb.append(option());
        sb.append(", scope=");
        sb.append(scope());
        sb.append(", label=");
        sb.append(label());
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
        sb.append(label());
        sb.append(" ");
        sb.append(itemCount);
        sb.append(",");
        sb.append(elapsedSec());
        sb.append(",");
        sb.append(elapsedRate());
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
