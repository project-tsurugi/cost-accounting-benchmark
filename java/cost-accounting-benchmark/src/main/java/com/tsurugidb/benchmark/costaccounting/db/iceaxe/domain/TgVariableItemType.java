package com.tsurugidb.benchmark.costaccounting.db.iceaxe.domain;

import com.tsurugidb.benchmark.costaccounting.db.doma2.domain.ItemType;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgParameter;
import com.tsurugidb.iceaxe.statement.TgVariable;

public class TgVariableItemType extends TgVariable<ItemType> {

    public static TgVariableItemType of(String name) {
        return new TgVariableItemType(name);
    }

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
