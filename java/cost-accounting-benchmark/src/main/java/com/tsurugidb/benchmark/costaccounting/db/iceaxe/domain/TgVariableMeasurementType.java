package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import com.tsurugidb.benchmark.costaccounting.db.doma2.domain.MeasurementType;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameter;
import com.tsurugidb.iceaxe.statement.TgVariable;

public class TgVariableMeasurementType extends TgVariable<MeasurementType> {

    protected TgVariableMeasurementType(String name) {
        super(name, TgDataType.CHARACTER);
    }

    @Override
    public TgParameter bind(MeasurementType value) {
        String v = (value != null) ? value.getValue() : null;
        return TgParameter.of(name(), v);
    }

    @Override
    public TgVariableMeasurementType copy(String name) {
        return new TgVariableMeasurementType(name);
    }
}
