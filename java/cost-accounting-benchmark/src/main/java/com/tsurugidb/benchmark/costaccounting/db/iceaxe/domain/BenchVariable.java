package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.time.LocalTime;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable.TgBindVariableInteger;

public class BenchVariable {

    public static TgBindVariableInteger ofInt(String name) {
        return TgBindVariable.ofInt(name);
    }

    public static TgBindVariable<LocalDate> ofDate(String name) {
        return TgBindVariable.ofDate(name);
    }

    public static TgBindVariable<LocalTime> ofTime(String name) {
        return TgBindVariable.ofTime(name);
    }

    public static TgBindVariable<String> ofString(String name) {
        return TgBindVariable.ofString(name);
    }

    public static TgBindVariable<BigDecimal> ofDecimal(String name, int scale) {
        return TgBindVariable.ofDecimal(name, scale, RoundingMode.HALF_UP);
    }

    public static TgBindVariable<BigInteger> ofBigInt(String name) {
        return new TgBindVariableBigInteger(name);
    }

    public static TgBindVariable<ItemType> ofItemType(String name) {
        return new TgBindVariableItemType(name);
    }

    public static TgBindVariable<MeasurementType> ofMeasurementType(String name) {
        return new TgBindVariableMeasurementType(name);
    }
}
