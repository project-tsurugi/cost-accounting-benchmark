package com.example.nedo.jdbc.raw.dao;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.BiConsumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.seasar.doma.jdbc.SqlKind;
import org.seasar.doma.jdbc.SqlLogType;
import org.seasar.doma.jdbc.UniqueConstraintException;
import org.seasar.doma.jdbc.criteria.statement.EmptySql;
import org.seasar.doma.jdbc.dialect.Dialect;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.raw.CostBenchDbManagerJdbc;
import com.example.nedo.jdbc.raw.dao.RawJdbcColumn.PsSetter;
import com.example.nedo.jdbc.raw.dao.RawJdbcColumn.RsGetter;

public abstract class RawJdbcDao<E> {
	protected final CostBenchDbManagerJdbc dbManager;
	private final String tableName;
	private final List<RawJdbcColumn<E, ?>> columnList;

	protected static <E, V> void add(List<RawJdbcColumn<E, ?>> list, String name, BiConsumer<E, V> entitySetter,
			Function<E, V> entityGetter, PsSetter<V> psSetter, RsGetter<V> rsGetter) {
		add(list, name, entitySetter, entityGetter, psSetter, rsGetter, false);
	}

	protected static <E, V> void add(List<RawJdbcColumn<E, ?>> list, String name, BiConsumer<E, V> entitySetter,
			Function<E, V> entityGetter, PsSetter<V> psSetter, RsGetter<V> rsGetter, boolean primaryKey) {
		list.add(new RawJdbcColumn<>(name, entitySetter, entityGetter, psSetter, rsGetter, primaryKey));
	}

	public RawJdbcDao(CostBenchDbManagerJdbc dbManager, String tableName, List<RawJdbcColumn<E, ?>> columnList) {
		this.dbManager = dbManager;
		this.tableName = tableName;
		this.columnList = columnList;
	}

	protected final Connection getConnection() {
		return dbManager.getConnection();
	}

	protected final PreparedStatement preparedStatement(String sql) {
		Connection c = getConnection();
		try {
			return c.prepareStatement(sql);
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	// execute statement

	@FunctionalInterface
	protected static interface SqlConsumer<T> {
		public void accept(T t) throws SQLException;
	}

	@FunctionalInterface
	protected static interface SqlBiConsumer<E, T> {
		public void accept(E entity, T t) throws SQLException;
	}

	@FunctionalInterface
	protected static interface SqlFunction<T, R> {
		public R apply(T t) throws SQLException;
	}

	protected final int executeQuery(String sql, SqlConsumer<PreparedStatement> prepare,
			SqlConsumer<ResultSet> converter) {
		try (PreparedStatement ps = preparedStatement(sql)) {
			if (prepare != null) {
				prepare.accept(ps);
			}
			int count = 0;
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					converter.accept(rs);
					count++;
				}
			}
			return count;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected final <R> R executeQuery1(String sql, SqlConsumer<PreparedStatement> prepare,
			SqlFunction<ResultSet, R> converter) {
		try (PreparedStatement ps = preparedStatement(sql)) {
			if (prepare != null) {
				prepare.accept(ps);
			}
			try (ResultSet rs = ps.executeQuery()) {
				while (rs.next()) {
					return converter.apply(rs);
				}
			}
			return null;
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected final <R> List<R> executeQueryList(String sql, SqlConsumer<PreparedStatement> prepare,
			SqlFunction<ResultSet, R> converter) {
		try (PreparedStatement ps = preparedStatement(sql)) {
			if (prepare != null) {
				prepare.accept(ps);
			}
			try (ResultSet rs = ps.executeQuery()) {
				List<R> list = new ArrayList<>();
				while (rs.next()) {
					list.add(converter.apply(rs));
				}
				return list;
			}
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	protected final <R> Stream<R> executeQueryStream(String sql, SqlConsumer<PreparedStatement> prepare,
			SqlFunction<ResultSet, R> converter) {
		PreparedStatement ps = null;
		ResultSet rs = null;
		Stream<R> stream;
		try {
			ps = preparedStatement(sql);
			if (prepare != null) {
				prepare.accept(ps);
			}

			Iterator<R> iterator;
			rs = ps.executeQuery();
			try {
				iterator = new ResultSetIterator<>(rs, converter);
			} catch (Exception e) {
				try {
					rs.close();
				} catch (Throwable t) {
					e.addSuppressed(t);
				} finally {
					ps.close();
				}
				throw e;
			}

			Spliterator<R> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
			stream = StreamSupport.stream(spliterator, false);
		} catch (SQLException e) {
			RuntimeException r = new RuntimeException(e);
			try {
				if (rs != null) {
					try {
						rs.close();
					} catch (Throwable t) {
						r.addSuppressed(t);
					}
				}
			} finally {
				if (ps != null) {
					try {
						ps.close();
					} catch (Throwable t) {
						r.addSuppressed(t);
					}
				}
			}
			throw r;
		}

		final PreparedStatement finalPs = ps;
		final ResultSet finalRs = rs;
		return stream.onClose(() -> {
			try (PreparedStatement ps0 = finalPs; ResultSet rs0 = finalRs) {
				// close by try-with-resources
			} catch (SQLException e) {
				new RuntimeException(e);
			}
		});
	}

	private static class ResultSetIterator<R> implements Iterator<R> {
		private final ResultSet rs;
		private final SqlFunction<ResultSet, R> converter;
		private boolean hasNext;

		public ResultSetIterator(ResultSet rs, SqlFunction<ResultSet, R> converter) throws SQLException {
			this.rs = rs;
			this.converter = converter;
			this.hasNext = rs.next();
		}

		@Override
		public boolean hasNext() {
			return hasNext;
		}

		@Override
		public R next() {
			try {
				if (hasNext) {
					R r = converter.apply(rs);
					this.hasNext = rs.next();
					return r;
				}
			} catch (SQLException e) {
				throw new RuntimeException(e);
			}
			throw new NoSuchElementException();
		}
	}

	protected final int executeUpdate(String sql, SqlConsumer<PreparedStatement> prepare) {
		try (PreparedStatement ps = preparedStatement(sql)) {
			if (prepare != null) {
				prepare.accept(ps);
			}
			return ps.executeUpdate();
		} catch (SQLException e) {
			Dialect dialect = AppConfig.singleton().getDialect();
			if (dialect.isUniqueConstraintViolated(e)) {
				throw new UniqueConstraintException(SqlLogType.RAW, new EmptySql(SqlKind.BATCH_UPDATE), e);
			}
			throw new RuntimeException(e);
		}
	}

	protected final int[] executeBatch(String sql, Collection<E> list, SqlBiConsumer<E, PreparedStatement> prepare) {
		try (PreparedStatement ps = preparedStatement(sql)) {
			for (E entity : list) {
				if (prepare != null) {
					prepare.accept(entity, ps);
				}
				ps.addBatch();
			}
			return ps.executeBatch();
		} catch (SQLException e) {
			throw new RuntimeException(e);
		}
	}

	// do sql

	protected final int doDeleteAll() {
		String sql = "truncate table " + tableName;
		return executeUpdate(sql, null);
	}

	private String insertSql;

	protected final int doInsert(E entity) {
		if (insertSql == null) {
			this.insertSql = getInsertSql(tableName, columnList);
		}
		return executeUpdate(insertSql, ps -> {
			int i = 1;
			for (RawJdbcColumn<E, ?> column : columnList) {
				column.setPs(ps, i++, entity);
			}
		});
	}

	protected final int[] doInsert(Collection<E> entityList) {
		if (insertSql == null) {
			this.insertSql = getInsertSql(tableName, columnList);
		}
		return executeBatch(insertSql, entityList, (entity, ps) -> {
			int i = 1;
			for (RawJdbcColumn<E, ?> column : columnList) {
				column.setPs(ps, i++, entity);
			}
		});
	}

	private static <E> String getInsertSql(String tableName, List<RawJdbcColumn<E, ?>> columnList) {
		String names = columnList.stream().map(c -> c.getName()).collect(Collectors.joining(","));
		String values = Stream.generate(() -> "?").limit(columnList.size()).collect(Collectors.joining(","));
		String sql = "insert into " + tableName + "(" + names + ") values (" + values + ")";
		return sql;
	}

	private String selectAllSql;

	protected final List<E> doSelectAll(Supplier<E> entitySupplier) {
		if (selectAllSql == null) {
			this.selectAllSql = "select * from " + tableName;
		}
		return executeQueryList(selectAllSql, null, rs -> {
			E entity = entitySupplier.get();
			fillEntity(entity, rs);
			return entity;
		});
	}

	protected final void fillEntity(E entity, ResultSet rs) throws SQLException {
		for (RawJdbcColumn<E, ?> column : columnList) {
			column.fillEntity(entity, rs);
		}
	}

	private String updateSql;

	protected final int doUpdate(E entity) {
		if (updateSql == null) {
			this.updateSql = getUpdateSql(tableName, columnList);
		}
		return executeUpdate(updateSql, ps -> {
			int i = 1;
			for (RawJdbcColumn<E, ?> column : columnList) {
				if (!column.isPrimaryKey()) {
					column.setPs(ps, i++, entity);
				}
			}
			for (RawJdbcColumn<E, ?> column : columnList) {
				if (column.isPrimaryKey()) {
					column.setPs(ps, i++, entity);
				}
			}
		});
	}

	private static <E> String getUpdateSql(String tableName, List<RawJdbcColumn<E, ?>> columnList) {
		String set = columnList.stream().filter(c -> !c.isPrimaryKey()).map(c -> c.getName() + "=?")
				.collect(Collectors.joining(","));
		String where = columnList.stream().filter(c -> c.isPrimaryKey()).map(c -> c.getName() + "=?")
				.collect(Collectors.joining(" and "));
		String sql = "update " + tableName + " set " + set + " where " + where;
		return sql;
	}
}
