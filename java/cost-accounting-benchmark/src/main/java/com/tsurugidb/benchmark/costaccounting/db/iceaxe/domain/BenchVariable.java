package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;

public class BenchVariable {

    public static TgVariableInteger ofInt(String name) {
        return TgVariable.ofInt4(name);
    }

    public static TgVariable<LocalDate> ofDate(String name) {
        return TgVariable.ofDate(name);
    }

    public static TgVariable<String> ofString(String name) {
        return TgVariable.ofCharacter(name);
    }

    public static TgVariable<BigDecimal> ofDecimal(String name, int scale) {
        return TgVariable.ofDecimal(name, scale, RoundingMode.HALF_UP);
    }

    public static TgVariable<BigInteger> ofBigInt(String name) {
        return new TgVariableBigInteger(name);
    }

    public static TgVariable<ItemType> ofItemType(String name) {
        return new TgVariableItemType(name);
    }

    public static TgVariable<MeasurementType> ofMeasurementType(String name) {
        return new TgVariableMeasurementType(name);
    }
}
