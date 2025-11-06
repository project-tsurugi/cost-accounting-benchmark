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
package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;

import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.ResultSet;

/**
 * column for Iceaxe
 *
 * @param <E> entity type
 * @param <V> value type
 */
public class TsubakuroColumn<E, V> {

    @FunctionalInterface
    public interface TsubakuroResultSetGetter<V> {
        V apply(ResultSet rs, int index) throws IOException, ServerException, InterruptedException;
    }

    private final String name;
    private final AtomType type;

    private final BiConsumer<E, V> entitySetter;
    private final Function<E, V> entityGetter;
    private final BiFunction<String, V, Parameter> parameterSupplier;
    private final TsubakuroResultSetGetter<V> resultSetGetter;
    private final boolean primaryKey;

    public TsubakuroColumn(String name, AtomType type, BiConsumer<E, V> entitySetter, Function<E, V> entityGetter, BiFunction<String, V, Parameter> parameterSupplier,
            TsubakuroResultSetGetter<V> resultSetGetter, boolean primaryKey) {
        this.name = name;
        this.type = type;
        this.entitySetter = entitySetter;
        this.entityGetter = entityGetter;
        this.parameterSupplier = parameterSupplier;
        this.resultSetGetter = resultSetGetter;
        this.primaryKey = primaryKey;
    }

    public String getName() {
        return this.name;
    }

    public String getSqlName() {
        return ":" + this.name;
    }

    public AtomType getType() {
        return this.type;
    }

    public Parameter getParameter(E entity) {
        V value = entityGetter.apply(entity);
        return parameterSupplier.apply(name, value);
    }

    public void getFromResultSet(ResultSet rs, int index, E entity) throws IOException, ServerException, InterruptedException {
        V value = resultSetGetter.apply(rs, index);
        entitySetter.accept(entity, value);
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }
}
