package com.tsurugidb.benchmark.costaccounting.db.domain;

public enum MeasurementType {
    LENGTH, CAPACITY, WEIGHT, OTHER;

    public static MeasurementType of(String value) {
        return MeasurementType.valueOf(value.toUpperCase());
    }

    public String getValue() {
        return name().toLowerCase();
    }
}
