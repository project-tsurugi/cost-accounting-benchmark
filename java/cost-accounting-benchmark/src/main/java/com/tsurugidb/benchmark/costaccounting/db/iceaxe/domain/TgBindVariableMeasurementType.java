package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import com.tsurugidb.benchmark.costaccounting.db.domain.MeasurementType;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

public class TgBindVariableMeasurementType extends TgBindVariable<MeasurementType> {

    protected TgBindVariableMeasurementType(String name) {
        super(name, TgDataType.STRING);
    }

    @Override
    public TgBindParameter bind(MeasurementType value) {
        String v = (value != null) ? value.getValue() : null;
        return TgBindParameter.of(name(), v);
    }

    @Override
    public TgBindVariableMeasurementType clone(String name) {
        return new TgBindVariableMeasurementType(name);
    }
}
