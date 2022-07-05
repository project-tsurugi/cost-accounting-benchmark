package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import java.math.BigDecimal;

import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameter;
import com.tsurugidb.iceaxe.statement.TgVariable;

//TODO dataType (deprecated class)
public class TmpVariableDecimal extends TgVariable<BigDecimal> {

    protected TmpVariableDecimal(String name) {
        super(name, TgDataType.FLOAT8);
    }

    @Override
    public TgParameter bind(BigDecimal value) {
        Double v = (value != null) ? value.doubleValue() : null;
        return TgParameter.of(name(), v);
    }

    @Override
    public TmpVariableDecimal copy(String name) {
        return new TmpVariableDecimal(name);
    }
}
