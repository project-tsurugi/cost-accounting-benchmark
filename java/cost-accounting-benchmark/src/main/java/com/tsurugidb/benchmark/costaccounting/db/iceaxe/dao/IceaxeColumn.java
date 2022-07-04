package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.util.function.BiConsumer;
import java.util.function.Function;

import com.tsurugidb.iceaxe.result.TsurugiResultRecord;
import com.tsurugidb.iceaxe.statement.TgDataType;
import com.tsurugidb.iceaxe.statement.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;

/**
 * column for Iceaxe
 *
 * @param <E> entity type
 * @param <V> value type
 */
public class IceaxeColumn<E, V> {

    @FunctionalInterface
    public interface RecordGetter<V> {
        public V get(TsurugiResultRecord record) throws IOException;
    }

    private final TgVariable<V> variable;

    private final BiConsumer<E, V> entitySetter;
    private final Function<E, V> entityGetter;
    private final RecordGetter<V> recordGetter;
    private final boolean primaryKey;

    public IceaxeColumn(TgVariable<V> variable, BiConsumer<E, V> entitySetter, Function<E, V> entityGetter, RecordGetter<V> recordGetter, boolean primaryKey) {
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

    public TgVariable<V> getVariable() {
        return variable;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    public void addTo(TgEntityParameterMapping<E> parameterMapping) {
        parameterMapping.add(variable, entityGetter);
    }

    // entity.setFId(record.getInt("f_id"));
    public void fillEntity(E entity, TsurugiResultRecord record) throws IOException {
        V value = recordGetter.get(record);
        entitySetter.accept(entity, value);
    }
}
