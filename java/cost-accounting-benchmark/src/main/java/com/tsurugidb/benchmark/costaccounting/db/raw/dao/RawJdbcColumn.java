package com.tsurugidb.benchmark.costaccounting.db.raw.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * column for raw JDBC
 *
 * @param <E> entity type
 * @param <V> value type
 */
public class RawJdbcColumn<E, V> {

    @FunctionalInterface
    public interface PsSetter<V> {
        public void set(PreparedStatement ps, int index, V value) throws SQLException;
    }

    @FunctionalInterface
    public interface RsGetter<V> {
        public V get(ResultSet rs, String name) throws SQLException;
    }

    private final String name;

    private final BiConsumer<E, V> entitySetter;
    private final Function<E, V> entityGetter;
    private final PsSetter<V> psSetter;
    private final RsGetter<V> rsGetter;
    private final boolean primaryKey;

    public RawJdbcColumn(String name, BiConsumer<E, V> entitySetter, Function<E, V> entityGetter, PsSetter<V> psSetter, RsGetter<V> rsGetter, boolean primaryKey) {
        this.name = name;
        this.entitySetter = entitySetter;
        this.entityGetter = entityGetter;
        this.psSetter = psSetter;
        this.rsGetter = rsGetter;
        this.primaryKey = primaryKey;
    }

    public String getName() {
        return this.name;
    }

    public boolean isPrimaryKey() {
        return primaryKey;
    }

    // setInt(ps, i++, entity.getFId());
    public void setPs(PreparedStatement ps, int index, E entity) throws SQLException {
        V value = entityGetter.apply(entity);
        psSetter.set(ps, index, value);
    }

    // entity.setFId(getInt(rs, "f_id"));
    public void fillEntity(E entity, ResultSet rs) throws SQLException {
        V value = rsGetter.get(rs, name);
        entitySetter.accept(entity, value);
    }
}
