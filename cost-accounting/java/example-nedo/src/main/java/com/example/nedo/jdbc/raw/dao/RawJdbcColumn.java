package com.example.nedo.jdbc.raw.dao;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.function.BiConsumer;
import java.util.function.Function;

public class RawJdbcColumn<E, V> {
	@FunctionalInterface
	public static interface PsSetter<V> {
		public void set(PreparedStatement ps, int index, V value) throws SQLException;
	}

	@FunctionalInterface
	public static interface RsGetter<V> {
		public V get(ResultSet rs, String name) throws SQLException;
	}

	private final String name;

	private final BiConsumer<E, V> entitySetter;
	private final Function<E, V> entityGetter;
	private final PsSetter<V> psSetter;
	private final RsGetter<V> rsGetter;

	public RawJdbcColumn(String name, BiConsumer<E, V> entitySetter, Function<E, V> entityGetter, PsSetter<V> psSetter,
			RsGetter<V> rsGetter) {
		this.name = name;
		this.entitySetter = entitySetter;
		this.entityGetter = entityGetter;
		this.psSetter = psSetter;
		this.rsGetter = rsGetter;
	}

	public String getName() {
		return this.name;
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
