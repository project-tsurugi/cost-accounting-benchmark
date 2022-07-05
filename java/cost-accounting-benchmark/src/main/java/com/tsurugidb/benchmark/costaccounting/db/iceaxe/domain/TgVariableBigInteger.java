package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameter;
import com.tsurugidb.iceaxe.statement.TgVariable;

public class TgVariableBigInteger extends TgVariable<BigInteger> {

    protected TgVariableBigInteger(String name) {
        super(name, TgDataType.DECIMAL);
    }

    @Override
    public TgParameter bind(BigInteger value) {
        BigDecimal v = (value != null) ? new BigDecimal(value) : null;
        return TgParameter.of(name(), v);
    }

    @Override
    public TgVariableBigInteger copy(String name) {
        return new TgVariableBigInteger(name);
    }
}
