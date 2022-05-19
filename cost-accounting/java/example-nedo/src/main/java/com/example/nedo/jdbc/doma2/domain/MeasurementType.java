package com.example.nedo.jdbc.doma2.domain;

import org.seasar.doma.Domain;

@Domain(valueType = String.class, factoryMethod = "of")
public enum MeasurementType {
    LENGTH, CAPACITY, WEIGHT, OTHER;

    public static MeasurementType of(String value) {
        return MeasurementType.valueOf(value.toUpperCase());
    }

    public String getValue() {
        return name().toLowerCase();
    }
}
