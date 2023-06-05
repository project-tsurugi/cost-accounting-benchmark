package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.text.MessageFormat;
import java.util.ArrayList;
import java.util.Arrays;
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
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.sql.TsurugiSql;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlPreparedStatement;
import com.tsurugidb.iceaxe.sql.TsurugiSqlQuery;
import com.tsurugidb.iceaxe.sql.TsurugiSqlStatement;
import com.tsurugidb.iceaxe.sql.explain.TgStatementMetadata;
import com.tsurugidb.iceaxe.sql.parameter.TgBindParameters;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;
import com.tsurugidb.iceaxe.sql.parameter.TgParameterMapping;
import com.tsurugidb.iceaxe.sql.parameter.mapping.TgEntityParameterMapping;
import com.tsurugidb.iceaxe.sql.result.TgResultMapping;
import com.tsurugidb.iceaxe.sql.result.TsurugiStatementResult;
import com.tsurugidb.iceaxe.sql.result.mapping.TgEntityResultMapping;
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

    protected static <E, T> void add(List<IceaxeColumn<E, ?>> list, TgBindVariable<T> variable, BiConsumer<E, T> entitySetter, Function<E, T> entityGetter, RecordGetter<T> recordGetter) {
        add(list, variable, entitySetter, entityGetter, recordGetter, false);
    }

    protected static <E, T> void add(List<IceaxeColumn<E, ?>> list, TgBindVariable<T> variable, BiConsumer<E, T> entitySetter, Function<E, T> entityGetter, RecordGetter<T> recordGetter,
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
        try (var ps = session.createStatement(sql)) {
            var transaction = getTransaction();
            return transaction.executeAndGetCount(ps);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final int doInsert(E entity) {
        var ps = insertCache.get();
        return executeAndGetCount(ps, entity);
    }

    protected final int[] doInsert(Collection<E> entityList) {
        // TODO batch insertに切り替え
        switch (1) {
        default:
        case 0:
            return doInsertWait(entityList);
        case 1:
            return doInsertNoWait(entityList);
        }
    }

    private int[] doInsertWait(Collection<E> entityList) {
        var ps = insertCache.get();
        var result = new int[entityList.size()];
        int i = 0;
        for (var entity : entityList) {
            result[i++] = executeAndGetCount(ps, entity);
        }
        return result;
    }

    private int[] doInsertNoWait(Collection<E> entityList) {
        var ps = insertCache.get();
        return executeAndGetCount(ps, entityList);
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
            var session = getSession();
            return psMap.computeIfAbsent(session, k -> {
                synchronized (this) {
                    if (this.sql == null) {
                        initialize();
                    }
                }
                try {
                    return generate(session);
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            });
        }

        protected abstract void initialize();

        protected abstract S generate(TsurugiSession session) throws IOException, InterruptedException;
    }

    protected abstract class CacheStatement extends AbstractCache<TsurugiSqlStatement> {

        @Override
        protected TsurugiSqlStatement generate(TsurugiSession session) throws IOException {
            return session.createStatement(sql);
        }
    }

    protected abstract class CachePreparedStatement<P> extends AbstractCache<TsurugiSqlPreparedStatement<P>> {
        protected TgParameterMapping<P> parameterMapping;

        @Override
        protected TsurugiSqlPreparedStatement<P> generate(TsurugiSession session) throws IOException, InterruptedException {
            return session.createStatement(sql, parameterMapping);
        }
    }

    @SuppressWarnings("serial")
    private static class ExceptionInfo extends Throwable {
        public ExceptionInfo(String message) {
            super(message);
        }
    }

    protected final <P> int executeAndGetCount(TsurugiSqlPreparedStatement<P> ps, P parameter) {
        debugExplain(ps, () -> ps.explain(parameter));
        try {
            var transaction = getTransaction();
            return transaction.executeAndGetCount(ps, parameter);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            if (isUniqueConstraint(e)) {
                throw new UniqueConstraintException(e);
            }
            var r = new TsurugiTransactionRuntimeException(e);
            if (!dbManager.isRetryable(e)) {
                String message = MessageFormat.format("sql={0}, parameter={1}", ps.getSql(), parameter);
                r.addSuppressed(new ExceptionInfo(message));
            }
            throw r;
        }
    }

    private final <P> int[] executeAndGetCount(TsurugiSqlPreparedStatement<P> ps, Collection<P> parameterList) {
        RuntimeException re = null;
        var rcList = new ArrayList<TsurugiStatementResult>(parameterList.size());
        try {
            var transaction = getTransaction();
            for (var parameter : parameterList) {
                debugExplain(ps, () -> ps.explain(parameter));
                try {
                    var rc = transaction.executeStatement(ps, parameter);
                    rcList.add(rc);
                } catch (IOException e) {
                    re = new UncheckedIOException(e.getMessage(), e);
                    throw re;
                } catch (InterruptedException e) {
                    re = new RuntimeException(e);
                    throw re;
                } catch (TsurugiTransactionException e) {
                    if (isUniqueConstraint(e)) {
                        re = new UniqueConstraintException(e);
                        throw re;
                    }
                    re = new TsurugiTransactionRuntimeException(e);
                    if (!dbManager.isRetryable(e)) {
                        String message = MessageFormat.format("sql={0}, parameter={1}", ps.getSql(), parameter);
                        re.addSuppressed(new ExceptionInfo(message));
                    }
                    throw re;
                }
            }
        } finally {
            for (var rc : rcList) {
                try {
                    rc.close();
                } catch (IOException e) {
                    if (re != null) {
                        re.addSuppressed(e);
                    } else {
                        re = new UncheckedIOException(e.getMessage(), e);
                    }
                } catch (TsurugiTransactionException e) {
                    if (re != null) {
                        re.addSuppressed(e);
                    } else {
                        re = new TsurugiTransactionRuntimeException(e);
                    }
                } catch (RuntimeException e) {
                    if (re != null) {
                        re.addSuppressed(e);
                    } else {
                        re = e;
                    }
                } catch (Exception e) {
                    if (re != null) {
                        re.addSuppressed(e);
                    } else {
                        re = new RuntimeException(e);
                    }
                }
            }
            if (re != null) {
                throw re;
            }
        }

        // TODO batch result
        var result = new int[parameterList.size()];
        Arrays.fill(result, -1);
        return result;
    }

    private boolean isUniqueConstraint(TsurugiTransactionException e) {
        var code = e.getDiagnosticCode();
        if (code == SqlServiceCode.ERR_ALREADY_EXISTS) {
            // 同一トランザクション内でinsertの一意制約違反
            return true;
        }
        return false;
    }

    protected abstract class CacheQuery<R> extends AbstractCache<TsurugiSqlQuery<R>> {
        protected TgResultMapping<R> resultMapping;

        @Override
        protected TsurugiSqlQuery<R> generate(TsurugiSession session) throws IOException {
            return session.createQuery(sql, resultMapping);
        }
    }

    protected final <R> List<R> executeAndGetList(TsurugiSqlQuery<R> ps) {
//        debugExplain(ps.getSql(), () -> ps.explain());
        try {
            var transaction = getTransaction();
            return transaction.executeAndGetList(ps);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
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

    protected abstract class CachePreparedQuery<P, R> extends AbstractCache<TsurugiSqlPreparedQuery<P, R>> {
        protected TgParameterMapping<P> parameterMapping;
        protected TgResultMapping<R> resultMapping;

        @Override
        protected TsurugiSqlPreparedQuery<P, R> generate(TsurugiSession session) throws IOException, InterruptedException {
            return session.createQuery(sql, parameterMapping, resultMapping);
        }
    }

    protected final <R> R executeAndGetRecord(TsurugiSqlQuery<R> ps) {
//        debugExplain(ps.getSql(), () -> ps.explain());
        try {
            var transaction = getTransaction();
            return transaction.executeAndFindRecord(ps).orElse(null);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            wipRetry(e);
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final <P, R> R executeAndGetRecord(TsurugiSqlPreparedQuery<P, R> ps, P parameter) {
        debugExplain(ps, () -> ps.explain(parameter));
        try {
            var transaction = getTransaction();
            return transaction.executeAndFindRecord(ps, parameter).orElse(null);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            wipRetry(e);
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final <P, R> List<R> executeAndGetList(TsurugiSqlPreparedQuery<P, R> ps, P parameter) {
        debugExplain(ps, () -> ps.explain(parameter));
        try {
            var transaction = getTransaction();
            return transaction.executeAndGetList(ps, parameter);
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            wipRetry(e);
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final <R> Stream<R> executeAndGetStream(TsurugiSqlQuery<R> ps) {
        debugExplain(ps, () -> ps.explain());
        try {
            var transaction = getTransaction();
            var rs = transaction.executeQuery(ps);
            return StreamSupport.stream(rs.spliterator(), false).onClose(() -> {
                try {
                    rs.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (TsurugiTransactionException e) {
                    throw new TsurugiTransactionRuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            wipRetry(e);
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    protected final <P, R> Stream<R> executeAndGetStream(TsurugiSqlPreparedQuery<P, R> ps, P parameter) {
        debugExplain(ps, () -> ps.explain(parameter));
        try {
            var transaction = getTransaction();
            var rs = transaction.executeQuery(ps, parameter);
            return StreamSupport.stream(rs.spliterator(), false).onClose(() -> {
                try {
                    rs.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e.getMessage(), e);
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                } catch (TsurugiTransactionException e) {
                    throw new TsurugiTransactionRuntimeException(e);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        } catch (TsurugiTransactionException e) {
            wipRetry(e);
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    private Set<String> debugExplainSet = null;

    @FunctionalInterface
    private interface ExplainSupplier {
        TgStatementMetadata get() throws IOException, InterruptedException;
    }

    protected void debugExplain(TsurugiSql ps, ExplainSupplier explain) {
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
        try (var ps = session.createQuery(sql, TgParameterMapping.of(), resultMapping)) {
            var transaction = getTransaction();
            try {
                transaction.executeAndForEach(ps, TgBindParameters.of(), entity -> entityConsumer.accept(entity));
            } catch (TsurugiTransactionException e) {
                throw new TsurugiTransactionRuntimeException(e);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}
