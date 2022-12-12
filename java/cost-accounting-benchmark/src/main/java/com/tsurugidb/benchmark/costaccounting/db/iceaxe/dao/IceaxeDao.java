package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.function.BiConsumer;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.UniqueConstraintException;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.IceaxeColumn.RecordGetter;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.result.TgEntityResultMapping;
import com.tsurugidb.iceaxe.result.TgResultMapping;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.statement.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.statement.TgParameterList;
import com.tsurugidb.iceaxe.statement.TgParameterMapping;
import com.tsurugidb.iceaxe.statement.TgVariable;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatement;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementQuery1;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate0;
import com.tsurugidb.iceaxe.statement.TsurugiPreparedStatementUpdate1;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.tsubakuro.explain.PlanGraph;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

public abstract class IceaxeDao<E> {
    protected final Logger LOG = LoggerFactory.getLogger(getClass());
    private static final boolean DEBUG_EXPLAIN = BenchConst.debugExplain();

    protected final CostBenchDbManagerIceaxe dbManager;
    private final String tableName;
    private final List<IceaxeColumn<E, ?>> columnList;
    private final Supplier<E> entitySupplier;

    protected static <E, T> void add(List<IceaxeColumn<E, ?>> list, TgVariable<T> variable, BiConsumer<E, T> entitySetter, Function<E, T> entityGetter, RecordGetter<T> recordGetter) {
        add(list, variable, entitySetter, entityGetter, recordGetter, false);
    }

    protected static <E, T> void add(List<IceaxeColumn<E, ?>> list, TgVariable<T> variable, BiConsumer<E, T> entitySetter, Function<E, T> entityGetter, RecordGetter<T> recordGetter,
            boolean primaryKey) {
        list.add(new IceaxeColumn<>(variable, entitySetter, entityGetter, recordGetter, primaryKey));
    }

    public IceaxeDao(CostBenchDbManagerIceaxe dbManager, String tableName, List<IceaxeColumn<E, ?>> columnList, Supplier<E> entitySupplier) {
        this.dbManager = dbManager;
        this.tableName = tableName;
        this.columnList = columnList;
        this.entitySupplier = entitySupplier;
    }

    protected final TsurugiSession getSession() {
        return dbManager.getSession();
    }

    protected final TsurugiTransaction getTransaction() {
        return dbManager.getCurrentTransaction();
    }

    // do sql

    protected final int doDeleteAll() {
        var sql = "delete from " + tableName;
        try (var ps = getSession().createPreparedStatement(sql)) {
            var transaction = getTransaction();
            return ps.executeAndGetCount(transaction);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final int doInsert(E entity) {
        var ps = getInsertPs();
        return executeAndGetCount(ps, entity);
    }

    protected final int[] doInsert(Collection<E> entityList) {
        var ps = getInsertPs();
        var result = new int[entityList.size()];
        int i = 0;
        for (var entity : entityList) {
            result[i++] = executeAndGetCount(ps, entity);
        }
        return result;
    }

    private TsurugiPreparedStatementUpdate1<E> insertPs;

    private synchronized TsurugiPreparedStatementUpdate1<E> getInsertPs() {
        if (this.insertPs == null) {
            var names = getColumnNames();
            var values = columnList.stream().map(c -> c.getSqlName()).collect(Collectors.joining(","));
            var sql = "insert into " + tableName + "(" + names + ") values (" + values + ")";
            var parameterMapping = getEntityParameterMapping();
            this.insertPs = createPreparedStatement(sql, parameterMapping);
        }
        return this.insertPs;
    }

    private TgEntityParameterMapping<E> getEntityParameterMapping() {
        var parameterMapping = TgEntityParameterMapping.<E>of();
        for (var column : columnList) {
            column.addTo(parameterMapping);
        }
        return parameterMapping;
    }

    protected final List<E> doSelectAll() {
        var ps = getSelectAllPs();
        return executeAndGetList(ps);
    }

    private TsurugiPreparedStatementQuery0<E> selectAllPs;

    private synchronized TsurugiPreparedStatementQuery0<E> getSelectAllPs() {
        if (this.selectAllPs == null) {
            var sql = getSelectEntitySql();
            var resultMapping = getEntityResultMapping();
            this.selectAllPs = createPreparedQuery(sql, resultMapping);
        }
        return this.selectAllPs;
    }

    protected final int doUpdate(E entity) {
        var ps = getUpdatePs();
        return executeAndGetCount(ps, entity);
    }

    private TsurugiPreparedStatementUpdate1<E> updatePs;

    private synchronized TsurugiPreparedStatementUpdate1<E> getUpdatePs() {
        if (this.updatePs == null) {
            var sql = new StringBuilder(128);
            sql.append("update ");
            sql.append(tableName);
            sql.append(" set");
            var comma = " ";
            for (var column : columnList) {
                if (!column.isPrimaryKey()) {
                    sql.append(comma);
                    comma = ",";
                    sql.append(column.getName());
                    sql.append("=");
                    sql.append(column.getSqlName());
                }
            }
            sql.append(" where ");
            var and = "";
            for (var column : columnList) {
                if (column.isPrimaryKey()) {
                    sql.append(and);
                    and = " and ";
                    sql.append(column.getName());
                    sql.append("=");
                    sql.append(column.getSqlName());
                }
            }
            var parameterMapping = getEntityParameterMapping();
            this.updatePs = createPreparedStatement(sql.toString(), parameterMapping);
        }
        return this.updatePs;
    }

    // common

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

    private TgEntityResultMapping<E> entityResultMapping;

    protected synchronized final TgResultMapping<E> getEntityResultMapping() {
        if (this.entityResultMapping == null) {
            var resultMapping = TgResultMapping.of(entitySupplier);
            for (var column : columnList) {
                resultMapping.add(column::fillEntity);
            }
            this.entityResultMapping = resultMapping;
        }
        return this.entityResultMapping;
    }

    // create/execute

    protected final TsurugiPreparedStatementUpdate0 createPreparedStatement(String sql) {
        try {
            return getSession().createPreparedStatement(sql);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    protected final <P> TsurugiPreparedStatementUpdate1<P> createPreparedStatement(String sql, TgParameterMapping<P> parameterMapping) {
        try {
            return getSession().createPreparedStatement(sql, parameterMapping);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    protected final <P> int executeAndGetCount(TsurugiPreparedStatementUpdate1<P> ps, P parameter) {
        debugExplain(ps, () -> ps.explain(parameter));
        try {
            var transaction = getTransaction();
            return ps.executeAndGetCount(transaction, parameter);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TsurugiTransactionException e) {
            if (isUniqueConstraint(e)) {
                throw new UniqueConstraintException(e);
            }
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    private boolean isUniqueConstraint(TsurugiTransactionException e) {
        var code = e.getDiagnosticCode();
        if (code == SqlServiceCode.ERR_ALREADY_EXISTS) {
            // 同一トランザクション内でinsertの一意制約違反
            return true;
        }
        return false;
    }

    protected final <R> TsurugiPreparedStatementQuery0<R> createPreparedQuery(String sql, TgResultMapping<R> resultMapping) {
        try {
            return getSession().createPreparedQuery(sql, resultMapping);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    protected final <R> List<R> executeAndGetList(TsurugiPreparedStatementQuery0<R> ps) {
//        debugExplain(ps.getSql(), () -> ps.explain());
        try {
            var transaction = getTransaction();
            return ps.executeAndGetList(transaction);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final <P, R> TsurugiPreparedStatementQuery1<P, R> createPreparedQuery(String sql, TgParameterMapping<P> parameterMapping, TgResultMapping<R> resultMapping) {
        try {
            return getSession().createPreparedQuery(sql, parameterMapping, resultMapping);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }

    protected final <R> R executeAndGetRecord(TsurugiPreparedStatementQuery0<R> ps) {
//        debugExplain(ps.getSql(), () -> ps.explain());
        try {
            var transaction = getTransaction();
            return ps.executeAndFindRecord(transaction).orElse(null);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final <P, R> R executeAndGetRecord(TsurugiPreparedStatementQuery1<P, R> ps, P parameter) {
        debugExplain(ps, () -> ps.explain(parameter));
        try {
            var transaction = getTransaction();
            return ps.executeAndFindRecord(transaction, parameter).orElse(null);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final <P, R> List<R> executeAndGetList(TsurugiPreparedStatementQuery1<P, R> ps, P parameter) {
        debugExplain(ps, () -> ps.explain(parameter));
        try {
            var transaction = getTransaction();
            return ps.executeAndGetList(transaction, parameter);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final <P, R> Stream<R> executeAndGetStream(TsurugiPreparedStatementQuery1<P, R> ps, P parameter) {
        debugExplain(ps, () -> ps.explain(parameter));
        try {
            var transaction = getTransaction();
            var rs = ps.execute(transaction, parameter);
            return StreamSupport.stream(rs.spliterator(), false).onClose(() -> {
                try {
                    rs.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                } catch (TsurugiTransactionException e) {
                    throw new TsurugiTransactionRuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    private Set<String> debugExplainSet = null;

    @FunctionalInterface
    private interface ExplainSupplier {
        TgStatementMetadata get() throws IOException;
    }

    protected void debugExplain(TsurugiPreparedStatement ps, ExplainSupplier explain) {
        if (!DEBUG_EXPLAIN) {
            return;
        }

        String sql = ps.getSql();
        synchronized (this) {
            if (this.debugExplainSet == null) {
                this.debugExplainSet = new HashSet<>();
            }
            if (debugExplainSet.contains(sql)) {
                return;
            }
            debugExplainSet.add(sql);
        }

        PlanGraph graph;
        try {
            ps.setExplainConnectTimeout(10, TimeUnit.SECONDS);
            graph = explain.get().getLowPlanGraph();
        } catch (Exception e) {
            LOG.warn("explain error. sql={}", sql, e);
            return;
        }
        LOG.info("explain {}\n{}", sql, graph);
    }

    protected void doForEach(Consumer<E> entityConsumer) {
        String key = columnList.stream().filter(c -> c.isPrimaryKey()).map(c -> c.getName()).collect(Collectors.joining(","));
        var sql = getSelectEntitySql() + " order by " + key;
        var resultMapping = getEntityResultMapping();
        try (var ps = createPreparedQuery(sql, TgParameterMapping.of(), resultMapping)) {
            var transaction = getTransaction();
            try (var rs = ps.execute(transaction, TgParameterList.of())) {
                rs.forEach(entityConsumer);
            } catch (TsurugiTransactionException e) {
                throw new TsurugiTransactionRuntimeException(e);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }
    }
}
