/*
 * Copyright 2023-2025 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
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
