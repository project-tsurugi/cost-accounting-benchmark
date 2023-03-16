package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameter;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

public class TgBindVariableItemType extends TgBindVariable<ItemType> {

    protected TgBindVariableItemType(String name) {
        super(name, TgDataType.STRING);
    }

    @Override
    public TgBindParameter bind(ItemType value) {
        String v = (value != null) ? value.getValue() : null;
        return TgBindParameter.of(name(), v);
    }

    @Override
    public TgBindVariableItemType clone(String name) {
        return new TgBindVariableItemType(name);
    }
}
