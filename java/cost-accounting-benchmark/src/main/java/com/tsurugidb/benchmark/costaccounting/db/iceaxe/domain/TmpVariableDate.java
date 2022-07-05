package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import java.time.LocalDate;

import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameter;
import com.tsurugidb.iceaxe.statement.TgVariable;

//TODO dataType (deprecated class)
public class TmpVariableDate extends TgVariable<LocalDate> {

    protected TmpVariableDate(String name) {
        super(name, TgDataType.CHARACTER);
    }

    @Override
    public TgParameter bind(LocalDate value) {
        String v = (value != null) ? value.toString() : null;
        return TgParameter.of(name(), v);
    }

    @Override
    public TmpVariableDate copy(String name) {
        return new TmpVariableDate(name);
    }
}
