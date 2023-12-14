package com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiConsumer;
import java.util.function.BiFunction;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.TsubakuroColumn.TsubakuroResultSetGetter;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.sql.proto.SqlCommon.AtomType;
import com.tsurugidb.sql.proto.SqlRequest.Parameter;
import com.tsurugidb.sql.proto.SqlRequest.Placeholder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.explain.PlanGraphException;
import com.tsurugidb.tsubakuro.explain.json.JsonPlanGraphLoader;
import com.tsurugidb.tsubakuro.sql.Placeholders;
import com.tsurugidb.tsubakuro.sql.PreparedStatement;
import com.tsurugidb.tsubakuro.sql.ResultSet;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;

public abstract class TsubakuroDao<E> {
    private final Logger LOG = LoggerFactory.getLogger(getClass());
    private static final boolean DEBUG_EXPLAIN = BenchConst.debugExplain();

    protected final CostBenchDbManagerTsubakuro dbManager;
    private final String tableName;
    private final List<TsubakuroColumn<E, ?>> columnList;
    private final Supplier<E> entitySupplier;
    protected final String insert;

    protected static <E, T> void add(List<TsubakuroColumn<E, ?>> list, String name, AtomType type, BiConsumer<E, T> entitySetter, Function<E, T> entityGetter,
            BiFunction<String, T, Parameter> parameterSupplier, TsubakuroResultSetGetter<T> resultSetGetter) {
        add(list, name, type, entitySetter, entityGetter, parameterSupplier, resultSetGetter, false);
    }

    protected static <E, T> void add(List<TsubakuroColumn<E, ?>> list, String name, AtomType type, BiConsumer<E, T> entitySetter, Function<E, T> entityGetter,
            BiFunction<String, T, Parameter> parameterSupplier, TsubakuroResultSetGetter<T> resultSetGetter, boolean primaryKey) {
        list.add(new TsubakuroColumn<>(name, type, entitySetter, entityGetter, parameterSupplier, resultSetGetter, primaryKey));
    }

    public TsubakuroDao(CostBenchDbManagerTsubakuro dbManager, String tableName, List<TsubakuroColumn<E, ?>> columnList, Supplier<E> entitySupplier) {
        this.dbManager = dbManager;
        this.tableName = tableName;
        this.columnList = columnList;
        this.entitySupplier = entitySupplier;
        this.insert = BenchConst.sqlInsert(dbManager.getPurpose());
    }

    protected final SqlClient getSqlClient() {
        return dbManager.getSqlClient();
    }

    protected final Transaction getTransaction() {
        return dbManager.getCurrentTransaction();
    }

    // do sql

    protected final void doTruncate() {
        doDeleteAll();
    }

    protected final int doDeleteAll() {
        var sql = "delete from " + tableName;
        try {
            var transaction = getTransaction();
            transaction.executeStatement(sql).await();
            return -1; // TODO 処理件数
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected final int doInsert(E entity, boolean insertOnly) {
        var ps = getInsertPs(insertOnly);
        return executeAndGetCount(ps, entity);
    }

    protected final int[] doInsert(Collection<E> entityList, boolean insertOnly) {
        var ps = getInsertPs(insertOnly);
        var result = new int[entityList.size()];
        int i = 0;
        for (var entity : entityList) {
            result[i++] = executeAndGetCount(ps, entity);
        }
        return result;
    }

    private PreparedStatement insertOnlyPs;
    private PreparedStatement insertPs;

    private synchronized PreparedStatement getInsertPs(boolean insertOnly) {
        if (insertOnly) {
            if (this.insertOnlyPs == null) {
                this.insertOnlyPs = createInsertPs("insert");
            }
            return this.insertOnlyPs;
        }

        if (this.insertPs == null) {
            this.insertPs = createInsertPs(this.insert);
        }
        return this.insertPs;
    }

    private PreparedStatement createInsertPs(String insert) {
        var names = getColumnNames();
        var values = columnList.stream().map(c -> c.getSqlName()).collect(Collectors.joining(","));
        var sql = insert + " into " + tableName + "(" + names + ") values (" + values + ")";
        var placeholders = getInsertPlaceholders();
        return createPreparedStatement(sql, placeholders);
    }

    private List<Placeholder> getInsertPlaceholders() {
        var placeholders = new ArrayList<Placeholder>(columnList.size());
        for (var column : columnList) {
            var ph = Placeholders.of(column.getName(), column.getType());
            placeholders.add(ph);
        }
        return placeholders;
    }

    protected final List<E> doSelectAll() {
        var sql = getSelectEntitySql();
        var converter = getSelectEntityConverter();
        return executeAndGetList(sql, converter);
    }

    private String selectEntitySql;

    protected final String getSelectEntitySql() {
        if (this.selectEntitySql == null) {
            var names = getColumnNames();
            this.selectEntitySql = "select " + names + " from " + tableName;
        }
        return this.selectEntitySql;
    }

    protected final String getColumnNames() {
        return columnList.stream().map(c -> c.getName()).collect(Collectors.joining(","));
    }

    protected final TsubakuroFunction<ResultSet, E> getSelectEntityConverter() {
        return rs -> {
            var entity = entitySupplier.get();
            for (int i = 0; rs.nextColumn(); i++) {
                var column = columnList.get(i);
                column.getFromResultSet(rs, i, entity);
            }
            return entity;
        };
    }

    protected PreparedStatement createPreparedStatement(String sql, List<Placeholder> placeholders) {
        var client = getSqlClient();
        try {
            return client.prepare(sql, placeholders).await();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    protected int executeAndGetCount(PreparedStatement ps, E entity) {
        var parameters = new ArrayList<Parameter>(columnList.size());
        for (var column : columnList) {
            var parameter = column.getParameter(entity);
            parameters.add(parameter);
        }

        return executeAndGetCount(ps, parameters);
    }

    protected int executeAndGetCount(PreparedStatement ps, List<Parameter> parameters) {
        var transaction = getTransaction();
        try {
            transaction.executeStatement(ps, parameters).await();
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return -1; // TODO 処理件数
    }

    @FunctionalInterface
    public interface TsubakuroFunction<T, R> {
        R apply(T t) throws IOException, ServerException, InterruptedException;
    }

    protected <R> List<R> executeAndGetList(String sql, TsubakuroFunction<ResultSet, R> converter) {
        var list = new ArrayList<R>();
        var transaction = getTransaction();
        try (var rs = transaction.executeQuery(sql).await()) {
            while (rs.nextRow()) {
                R result = converter.apply(rs);
                list.add(result);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    protected <R> List<R> executeAndGetList(PreparedStatement ps, List<Parameter> parameters, TsubakuroFunction<ResultSet, R> converter) {
        var list = new ArrayList<R>();
        var transaction = getTransaction();
        try (var rs = transaction.executeQuery(ps, parameters).await()) {
            while (rs.nextRow()) {
                R result = converter.apply(rs);
                list.add(result);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return list;
    }

    protected <R> R executeAndGetRecord(PreparedStatement ps, List<Parameter> parameters, TsubakuroFunction<ResultSet, R> converter) {
        var transaction = getTransaction();
        try (var rs = transaction.executeQuery(ps, parameters).await()) {
            while (rs.nextRow()) {
                R result = converter.apply(rs);
                return result;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return null;
    }

    private final Map<String, AtomicInteger> explainMap = new ConcurrentHashMap<>();

    protected void explain(String sql, PreparedStatement ps, List<Parameter> parameters) {
        if (!DEBUG_EXPLAIN) {
            return;
        }
        var counter = explainMap.computeIfAbsent(sql, k -> new AtomicInteger(0));
        if (counter.getAndIncrement() != 0) {
            return;
        }
        try {
            var explain = getSqlClient().explain(ps, parameters).await();
            var graph = JsonPlanGraphLoader.newBuilder().build().load(explain.getFormatId(), explain.getFormatVersion(), explain.getContents());
            LOG.info("explain {}\n{}", sql, graph);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        } catch (PlanGraphException e) {
            throw new RuntimeException(e);
        }
    }

    public void dumpExplainCounter() {
        explainMap.forEach((key, counter) -> {
            LOG.info("count={}, sql={}", counter, key);
        });
    }
}
