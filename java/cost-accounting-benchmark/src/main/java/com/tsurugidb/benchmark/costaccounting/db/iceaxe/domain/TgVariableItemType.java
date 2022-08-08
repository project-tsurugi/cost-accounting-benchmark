package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameter;
import com.tsurugidb.iceaxe.statement.TgVariable;

public class TgVariableItemType extends TgVariable<ItemType> {

    protected TgVariableItemType(String name) {
        super(name, TgDataType.CHARACTER);
    }

    @Override
    public TgParameter bind(ItemType value) {
        String v = (value != null) ? value.getValue() : null;
        return TgParameter.of(name(), v);
    }

    @Override
    public TgVariableItemType copy(String name) {
        return new TgVariableItemType(name);
    }
}
