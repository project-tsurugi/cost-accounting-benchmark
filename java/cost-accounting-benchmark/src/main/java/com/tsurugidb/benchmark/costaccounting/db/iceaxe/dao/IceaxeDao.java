package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.MessageFormat;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
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

    protected final void doTruncate() {
        doDeleteAll();
    }

    protected final int doDeleteAll() {
        var sql = "delete from " + tableName;
        var session = getSession();
        try (var ps = session.createPreparedStatement(sql)) {
            var transaction = getTransaction();
            return ps.executeAndGetCount(transaction);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final int doInsert(E entity) {
        var ps = insertCache.get();
        return executeAndGetCount(ps, entity);
    }

    protected final int[] doInsert(Collection<E> entityList) {
        var ps = insertCache.get();
        var result = new int[entityList.size()];
        int i = 0;
        for (var entity : entityList) {
            result[i++] = executeAndGetCount(ps, entity);
        }
        return result;
    }

    private final CachePreparedStatement<E> insertCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
            var names = getColumnNames();
            var values = columnList.stream().map(c -> c.getSqlName()).collect(Collectors.joining(","));
            this.sql = "insert into " + tableName + "(" + names + ") values (" + values + ")";
            this.parameterMapping = getEntityParameterMapping();
        }
    };

    private TgEntityParameterMapping<E> getEntityParameterMapping() {
        var parameterMapping = TgEntityParameterMapping.<E>of();
        for (var column : columnList) {
            column.addTo(parameterMapping);
        }
        return parameterMapping;
    }

    protected final List<E> doSelectAll() {
        var ps = selectAllCache.get();
        return executeAndGetList(ps);
    }

    private final CacheQuery<E> selectAllCache = new CacheQuery<>() {
        @Override
        protected void initialize() {
            this.sql = getSelectEntitySql();
            this.resultMapping = getEntityResultMapping();
        }
    };

    protected final int doUpdate(E entity) {
        var ps = updateCache.get();
        return executeAndGetCount(ps, entity);
    }

    private final CachePreparedStatement<E> updateCache = new CachePreparedStatement<>() {
        @Override
        protected void initialize() {
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
            this.sql = sql.toString();
            this.parameterMapping = getEntityParameterMapping();
        }
    };

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

    protected final TgResultMapping<E> getEntityResultMapping() {
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

    private abstract class AbstractCache<S> {
        private final Map<TsurugiSession, S> psMap = new ConcurrentHashMap<>();
        protected String sql;

        public S get() {
            synchronized (this) {
                if (this.sql == null) {
                    initialize();
                }
            }

            var session = getSession();
            return psMap.computeIfAbsent(session, k -> {
                try {
                    return generate(session);
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                }
            });
        }

        protected abstract void initialize();

        protected abstract S generate(TsurugiSession session) throws IOException;
    }

    protected abstract class CacheStatement extends AbstractCache<TsurugiPreparedStatementUpdate0> {

        @Override
        protected TsurugiPreparedStatementUpdate0 generate(TsurugiSession session) throws IOException {
            return session.createPreparedStatement(sql);
        }
    }

    protected abstract class CachePreparedStatement<P> extends AbstractCache<TsurugiPreparedStatementUpdate1<P>> {
        protected TgParameterMapping<P> parameterMapping;

        @Override
        protected TsurugiPreparedStatementUpdate1<P> generate(TsurugiSession session) throws IOException {
            return session.createPreparedStatement(sql, parameterMapping);
        }
    }

    @SuppressWarnings("serial")
    private static class ExceptionInfo extends Throwable {
        public ExceptionInfo(String message) {
            super(message);
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
            var r = new TsurugiTransactionRuntimeException(e);
            if (!dbManager.isRetriable(e)) {
                String message = MessageFormat.format("sql={0}, parameter={1}", ps.getSql(), parameter);
                r.addSuppressed(new ExceptionInfo(message));
            }
            throw r;
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

    protected abstract class CacheQuery<R> extends AbstractCache<TsurugiPreparedStatementQuery0<R>> {
        protected TgResultMapping<R> resultMapping;

        @Override
        protected TsurugiPreparedStatementQuery0<R> generate(TsurugiSession session) throws IOException {
            return session.createPreparedQuery(sql, resultMapping);
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
            wipRetry(e);
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    // TODO 暫定リトライ判定を廃止して正式版にする
    private void wipRetry(TsurugiTransactionException e) {
        if (e.getDiagnosticCode() == SqlServiceCode.ERR_ABORTED) {
            throw new TsurugiTransactionRuntimeException(new TsurugiTransactionException("(WIP) retry", SqlServiceCode.ERR_ABORTED_RETRYABLE, e));
        }
    }

    protected abstract class CachePreparedQuery<P, R> extends AbstractCache<TsurugiPreparedStatementQuery1<P, R>> {
        protected TgParameterMapping<P> parameterMapping;
        protected TgResultMapping<R> resultMapping;

        @Override
        protected TsurugiPreparedStatementQuery1<P, R> generate(TsurugiSession session) throws IOException {
            return session.createPreparedQuery(sql, parameterMapping, resultMapping);
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
            wipRetry(e);
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
            wipRetry(e);
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
            wipRetry(e);
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
            wipRetry(e);
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
        var session = getSession();
        try (var ps = session.createPreparedQuery(sql, TgParameterMapping.of(), resultMapping)) {
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
