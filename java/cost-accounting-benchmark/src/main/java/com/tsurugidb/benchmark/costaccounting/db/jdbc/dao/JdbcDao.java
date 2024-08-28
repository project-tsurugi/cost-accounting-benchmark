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
package com.tsurugidb.benchmark.costaccounting.db.jdbc.dao;

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
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.postgresql.util.PSQLState;

import com.tsurugidb.benchmark.costaccounting.db.UniqueConstraintException;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcColumn.PsSetter;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.dao.JdbcColumn.RsGetter;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public abstract class JdbcDao<E> {
    protected final CostBenchDbManagerJdbc dbManager;
    private final String tableName;
    private final List<JdbcColumn<E, ?>> columnList;

    protected static <E, V> void add(List<JdbcColumn<E, ?>> list, String name, BiConsumer<E, V> entitySetter, Function<E, V> entityGetter, PsSetter<V> psSetter, RsGetter<V> rsGetter) {
        add(list, name, entitySetter, entityGetter, psSetter, rsGetter, false);
    }

    protected static <E, V> void add(List<JdbcColumn<E, ?>> list, String name, BiConsumer<E, V> entitySetter, Function<E, V> entityGetter, PsSetter<V> psSetter, RsGetter<V> rsGetter,
            boolean primaryKey) {
        list.add(new JdbcColumn<>(name, entitySetter, entityGetter, psSetter, rsGetter, primaryKey));
    }

    public JdbcDao(CostBenchDbManagerJdbc dbManager, String tableName, List<JdbcColumn<E, ?>> columnList) {
        this.dbManager = dbManager;
        this.tableName = tableName;
        this.columnList = columnList;
    }

    protected final PreparedStatement preparedStatement(String sql) {
        return dbManager.preparedStatement(sql);
    }

    // execute statement

    @FunctionalInterface
    protected interface SqlConsumer<T> {
        public void accept(T t) throws SQLException;
    }

    @FunctionalInterface
    protected interface SqlBiConsumer<E, T> {
        public void accept(E entity, T t) throws SQLException;
    }

    @FunctionalInterface
    protected interface SqlFunction<T, R> {
        public R apply(T t) throws SQLException;
    }

    protected final int executeQuery(String sql, SqlConsumer<PreparedStatement> prepare, SqlConsumer<ResultSet> converter) {
        var ps = preparedStatement(sql);
        try {
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

    protected final <R> R executeQuery1(String sql, SqlConsumer<PreparedStatement> prepare, SqlFunction<ResultSet, R> converter) {
        var ps = preparedStatement(sql);
        try {
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

    protected final <R> List<R> executeQueryList(String sql, SqlConsumer<PreparedStatement> prepare, SqlFunction<ResultSet, R> converter) {
        var ps = preparedStatement(sql);
        try {
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

    protected final <R> Stream<R> executeQueryStream(String sql, SqlConsumer<PreparedStatement> prepare, SqlFunction<ResultSet, R> converter) {
        ResultSet rs = null;
        Stream<R> stream;
        var ps = preparedStatement(sql);
        try {
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
                }
                throw e;
            }

            Spliterator<R> spliterator = Spliterators.spliteratorUnknownSize(iterator, 0);
            stream = StreamSupport.stream(spliterator, false);
        } catch (SQLException e) {
            RuntimeException r = new RuntimeException(e);
            if (rs != null) {
                try {
                    rs.close();
                } catch (Throwable t) {
                    r.addSuppressed(t);
                }
            }
            throw r;
        }

        final ResultSet finalRs = rs;
        return stream.onClose(() -> {
            try (ResultSet rs0 = finalRs) {
                // close by try-with-resources
            } catch (SQLException e) {
                throw new RuntimeException(e);
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
        var ps = preparedStatement(sql);
        try {
            if (prepare != null) {
                prepare.accept(ps);
            }
            return ps.executeUpdate();
        } catch (SQLException e) {
            if (isUniqueConstraint(e)) {
                throw new UniqueConstraintException(e);
            }
            throw new RuntimeException(e);
        }
    }

    private boolean isUniqueConstraint(SQLException e) {
        var type = BenchConst.dbmsType();
        switch (type) {
        case ORACLE:
            int code = e.getErrorCode();
            return code == 1; // ORA-0001
        case POSTGRESQL:
            String state = e.getSQLState();
            return PSQLState.UNIQUE_VIOLATION.getState().equals(state);
        default:
            throw new UnsupportedOperationException("unsupported dbms. type=" + type);
        }
    }

    protected final int[] executeBatch(String sql, Collection<E> list, SqlBiConsumer<E, PreparedStatement> prepare) {
        var ps = preparedStatement(sql);
        try {
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

    protected final void doTruncate() {
        String sql = "truncate table " + tableName;
        executeUpdate(sql, null);
    }

    protected final int doDeleteAll() {
        String sql = "delete from " + tableName;
        return executeUpdate(sql, null);
    }

    private String insertSql;

    protected final int doInsert(E entity) {
        if (insertSql == null) {
            this.insertSql = getInsertSql(tableName, columnList);
        }
        return executeUpdate(insertSql, ps -> {
            int i = 1;
            for (JdbcColumn<E, ?> column : columnList) {
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
            for (JdbcColumn<E, ?> column : columnList) {
                column.setPs(ps, i++, entity);
            }
        });
    }

    private static <E> String getInsertSql(String tableName, List<JdbcColumn<E, ?>> columnList) {
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
        for (JdbcColumn<E, ?> column : columnList) {
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
            for (JdbcColumn<E, ?> column : columnList) {
                if (!column.isPrimaryKey()) {
                    column.setPs(ps, i++, entity);
                }
            }
            for (JdbcColumn<E, ?> column : columnList) {
                if (column.isPrimaryKey()) {
                    column.setPs(ps, i++, entity);
                }
            }
        });
    }

    private static <E> String getUpdateSql(String tableName, List<JdbcColumn<E, ?>> columnList) {
        String set = columnList.stream().filter(c -> !c.isPrimaryKey()).map(c -> c.getName() + "=?").collect(Collectors.joining(","));
        String where = columnList.stream().filter(c -> c.isPrimaryKey()).map(c -> c.getName() + "=?").collect(Collectors.joining(" and "));
        String sql = "update " + tableName + " set " + set + " where " + where;
        return sql;
    }

    protected void doForEach(Supplier<E> entitySupplier, Consumer<E> entityConsumer) {
        String names = columnList.stream().map(c -> c.getName()).collect(Collectors.joining(","));
        String key = columnList.stream().filter(c -> c.isPrimaryKey()).map(c -> c.getName()).collect(Collectors.joining(","));
        String sql = "select " + names + " from " + tableName + " order by " + key;

        E entity = entitySupplier.get();
        var ps = preparedStatement(sql);
        try (ResultSet rs = ps.executeQuery()) {
            while (rs.next()) {
                fillEntity(entity, rs);
                entityConsumer.accept(entity);
            }
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}
