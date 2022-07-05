package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TgVariable.TgVariableInteger;

//TODO dataType
public class BenchVariable {

    public static TgVariableInteger ofInt(String name) {
        return TgVariable.ofInt4(name);
    }

    public static TgVariable<LocalDate> ofDate(String name) {
//      return TgVariable.ofDate(name);
        return new TmpVariableDate(name);
    }

    public static TgVariable<String> ofString(String name) {
        return TgVariable.ofCharacter(name);
    }

    public static TgVariable<BigDecimal> ofDecimal(String name) {
//      return TgVariable.ofDecimal(name);
        return new TmpVariableDecimal(name);
    }

    public static TgVariable<BigInteger> ofBigInt(String name) {
//      return new TgVariableBigInteger(name);
        return new TmpVariableBigInteger(name);
    }

    public static TgVariable<MeasurementType> ofMeasurementType(String name) {
        return new TgVariableMeasurementType(name);
    }

    public static TgVariable<ItemType> ofItemType(String name) {
        return new TgVariableItemType(name);
    }
}
