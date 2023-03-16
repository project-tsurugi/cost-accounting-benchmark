package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import java.math.BigDecimal;
import java.math.BigInteger;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

public class TgBindVariableBigInteger extends TgBindVariable<BigInteger> {

    protected TgBindVariableBigInteger(String name) {
        super(name, TgDataType.DECIMAL);
    }

    @Override
    public TgBindParameter bind(BigInteger value) {
        BigDecimal v = (value != null) ? new BigDecimal(value) : null;
        return TgBindParameter.of(name(), v);
    }

    @Override
    public TgBindVariableBigInteger clone(String name) {
        return new TgBindVariableBigInteger(name);
    }
}
