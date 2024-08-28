/*
 * Copyright 2023-2024 Project Tsurugi.
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
package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.tsurugidb.iceaxe.sql.TgDataType;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;

/**
 * column for Iceaxe
 *
 * @param <E> entity type
 * @param <V> value type
 */
public class IceaxeColumn<E, V> {

    @FunctionalInterface
    public interface RecordGetter<V> {
        public V get(TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException;
    }

    private final TgBindVariable<V> variable;

    private final BiConsumer<E, V> entitySetter;
    private final Function<E, V> entityGetter;
    private final RecordGetter<V> recordGetter;
    private final boolean primaryKey;

    public IceaxeColumn(TgBindVariable<V> variable, BiConsumer<E, V> entitySetter, Function<E, V> entityGetter, RecordGetter<V> recordGetter, boolean primaryKey) {
        this.variable = variable;
        this.entitySetter = entitySetter;
        this.entityGetter = entityGetter;
        this.recordGetter = recordGetter;
        this.primaryKey = primaryKey;
    }

    public String getName() {
        return variable.name();
    }

    public String getSqlName() {
        return variable.sqlName();
    }

    public TgDataType getType() {
        return variable.type();
    }

    public TgBindVariable<V> getVariable() {
        return variable;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void addTo(TgEntityParameterMapping<E> parameterMapping) {
        parameterMapping.add(variable, entityGetter);
    }

    // entity.setFId(record.getInt("f_id"));
    public void fillEntity(E entity, TsurugiResultRecord record) throws IOException, InterruptedException, TsurugiTransactionException {
        V value = recordGetter.get(record);
        entitySetter.accept(entity, value);
    }
}
