package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import java.math.BigInteger;

import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameter;
import com.tsurugidb.iceaxe.statement.TgVariable;

//TODO dataType (deprecated class)
public class TmpVariableBigInteger extends TgVariable<BigInteger> {

    protected TmpVariableBigInteger(String name) {
        super(name, TgDataType.FLOAT8);
    }

    @Override
    public TgParameter bind(BigInteger value) {
        Double v = (value != null) ? value.doubleValue() : null;
        return TgParameter.of(name(), v);
    }

    @Override
    public TmpVariableBigInteger copy(String name) {
        return new TmpVariableBigInteger(name);
    }
}
