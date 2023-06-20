package com.tsurugidb.benchmark.costaccounting.batch.command;

public class BatchRecordPart {

    private final String line;
    String dbmsType;
    String option;
    String onlineCoverRate;
    String scope;
    String label;
    double elapsed;
    String vsz;
    String rss;

    public BatchRecordPart(String line) {
        this.line = line;
    }

    public String getDbmsType() {
        return dbmsType;
    }

    public String getOption() {
        return option;
    }

    public String getOnlineCoverRate() {
        return onlineCoverRate;
    }

    public String getScope() {
        return scope;
    }

    public String getLabel() {
        return label;
    }

    public double getElapsed() {
        return elapsed;
    }

    public String getVsz() {
        return vsz;
    }

    public String getRss() {
        return rss;
    }

    @Override
    public String toString() {
        return "[" + this.line + "]";
    }
}
