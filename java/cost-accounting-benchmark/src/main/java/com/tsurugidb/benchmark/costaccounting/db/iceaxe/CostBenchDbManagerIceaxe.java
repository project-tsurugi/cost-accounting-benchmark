package com.tsurugidb.benchmark.costaccounting.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.UniqueConstraintException;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.CostMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.FactoryMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.IceaxeDao;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemConstructionMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemManufacturingMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.MeasurementMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ResultTableDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.StockHistoryDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.exception.TsurugiExceptionUtil;
import com.tsurugidb.iceaxe.session.TgSessionOption;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.session.event.logging.file.TsurugiSessionTxFileLogConfig;
import com.tsurugidb.iceaxe.transaction.TgCommitType;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.event.TsurugiTransactionEventListener;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.manager.exception.TsurugiTmIOException;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;

public class CostBenchDbManagerIceaxe extends CostBenchDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManagerIceaxe.class);

    private final AtomicBoolean closed = new AtomicBoolean(false);
    private final TsurugiConnector connector;
    private final TgSessionOption sessionOption;
    private final TsurugiTransactionManager singleTransactionManager;
    private final List<TsurugiSession> sessionList = new CopyOnWriteArrayList<>();
    private final ThreadLocal<TsurugiTransactionManager> transactionManagerThreadLocal = new ThreadLocal<>() {
        @Override
        protected TsurugiTransactionManager initialValue() {
            if (closed.get()) {
                throw new IllegalStateException(Thread.currentThread().getName() + " CostBenchDbManagerIceaxe already closed");
            }
            try {
                var session = connector.createSession(sessionOption);
                sessionList.add(session);
                LOG.debug("create session. sessionList.size={}", sessionList.size());
                var tm = session.createTransactionManager();
                tm.addEventListener(counter);
                return tm;
            } catch (IOException e) {
                LOG.info("sessionList.size={}", sessionList.size());
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }
    };

    private TsurugiTransaction singleTransaction;
    private final ThreadLocal<TsurugiTransaction> transactionThreadLocal = new ThreadLocal<>();

    public CostBenchDbManagerIceaxe(DbManagerPurpose purpose, boolean isMultiSession) {
        super("ICEAXE", true, purpose);
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        LOG.info("isMultiSession={}", isMultiSession);
        var credential = new UsernamePasswordCredential(BenchConst.tsurugiUser(), BenchConst.tsurugiPassword());
        this.connector = TsurugiConnector.of(endpoint, credential);
        try {
            this.sessionOption = TgSessionOption.of().setApplicationName("CostBenchDbManagerIceaxe");
            if (!isMultiSession) {
                var session = connector.createSession(sessionOption);
                var tm = session.createTransactionManager();
                tm.addEventListener(counter);
                this.singleTransactionManager = tm;
                sessionList.add(session);
            } else {
                this.singleTransactionManager = null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e.getMessage(), e);
        }

        String target = System.getProperty("bench.tx.log.target");
        if (target != null) {
            var stream = Arrays.stream(target.split(",")).map(String::trim);
            if (stream.anyMatch(s -> s.equalsIgnoreCase(purpose.name()))) {
                connector.setTxFileLogConfig(TsurugiSessionTxFileLogConfig.DEFAULT);
            } else {
                connector.setTxFileLogConfig(null);
            }
        }
    }

    private boolean isMultiSession() {
        return singleTransactionManager == null;
    }

    public TsurugiSession getSession() {
        return getTransactionManager().getSession();
    }

    protected TsurugiTransactionManager getTransactionManager() {
        return (this.singleTransactionManager != null) ? singleTransactionManager : transactionManagerThreadLocal.get();
    }

    private void setCurrentTransaction(TsurugiTransaction transaction) {
        if (isSingleTransaction()) {
            this.singleTransaction = transaction;
            return;
        }
        transactionThreadLocal.set(transaction);
    }

    public TsurugiTransaction getCurrentTransaction() {
        if (isSingleTransaction()) {
            return this.singleTransaction;
        }
        return transactionThreadLocal.get();
    }

    private void removeCurrentTransaction() {
        if (isSingleTransaction()) {
            this.singleTransaction = null;
            return;
        }
        transactionThreadLocal.remove();
    }

    @Override
    protected MeasurementMasterDao newMeasurementMasterDao() {
        return new MeasurementMasterDaoIceaxe(this);
    }

    @Override
    protected FactoryMasterDao newFactoryMasterDao() {
        return new FactoryMasterDaoIceaxe(this);
    }

    @Override
    protected ItemMasterDao newItemMasterDao() {
        return new ItemMasterDaoIceaxe(this);
    }

    @Override
    protected ItemConstructionMasterDao newItemConstructionMasterDao() {
        return new ItemConstructionMasterDaoIceaxe(this);
    }

    @Override
    protected ItemManufacturingMasterDao newItemManufacturingMasterDao() {
        return new ItemManufacturingMasterDaoIceaxe(this);
    }

    @Override
    protected CostMasterDao newCostMasterDao() {
        return new CostMasterDaoIceaxe(this);
    }

    @Override
    protected StockHistoryDao newStockHistoryDao() {
        return new StockHistoryDaoIceaxe(this);
    }

    @Override
    protected ResultTableDao newResultTableDao() {
        return new ResultTableDaoIceaxe(this);
    }

    @Override
    public void executeDdl(String... sqls) {
        var dao = new IceaxeDao<Object>(this, null, null, null) {
            public void executeDdl() {
                var setting = TgTmSetting.of(getOption());
                var tm = getTransactionManager();
                var session = tm.getSession();
                for (var sql : sqls) {
                    if (sql.startsWith("drop table")) {
                        int n = sql.lastIndexOf(" ");
                        String tableName = sql.substring(n + 1).trim();
                        try {
                            var opt = session.findTableMetadata(tableName);
                            if (opt.isEmpty()) {
                                continue;
                            }
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    try {
                        tm.executeDdl(setting, sql);
                    } catch (IOException e) {
                        LOG.info("ddl={}", sql.trim());
                        if (sql.equals(sqls[sqls.length - 1])) {
                            throw new UncheckedIOException(e);
                        }
                        LOG.warn("executeDdl exception={}", e.getMessage());
                    } catch (InterruptedException e) {
                        throw new RuntimeException(e);
                    }
                }
            }

            private TgTxOption getOption() {
                if (BenchConst.initTsurugiTxOption().equalsIgnoreCase("OCC")) {
                    return TgTxOption.ofOCC();
                } else {
                    return TgTxOption.ofLTX();
                }
            }
        };
        dao.executeDdl();
    }

    @Override
    public void execute(TgTmSetting setting, Runnable runnable) {
        var tm = getTransactionManager();
        try {
            tm.execute(setting, transaction -> {
                setCurrentTransaction(transaction);
                LOG.debug("setCurrentTransaction {}", transaction);
                try {
                    runnable.run();
                } finally {
                    removeCurrentTransaction();
                    LOG.debug("removeCurrentTransaction {}", transaction);
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(Thread.currentThread().getName() + " " + e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(Thread.currentThread().getName() + " " + e.getMessage(), e);
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        var tm = getTransactionManager();
        try {
            return tm.execute(setting, transaction -> {
                setCurrentTransaction(transaction);
                LOG.debug("setCurrentTransaction {}", transaction);
                try {
                    return supplier.get();
                } finally {
                    removeCurrentTransaction();
                    LOG.debug("removeCurrentTransaction {}", transaction);
                }
            });
        } catch (TsurugiTmIOException e) {
            var exceptionUtil = TsurugiExceptionUtil.getInstance();
            if (exceptionUtil.isUniqueConstraintViolation(e)) {
                throw new UniqueConstraintException(e);
            }
            if (exceptionUtil.isSerializationFailure(e)) {
                String message = e.getMessage();
                if (message.contains("reason_code:KVS_INSERT")) {
                    throw new UniqueConstraintException(e);
                }
            }
            var c = e.getCause();
            if (c instanceof UniqueConstraintException) {
                throw (UniqueConstraintException) c;
            }
            throw new UncheckedIOException(Thread.currentThread().getName() + " " + e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException(Thread.currentThread().getName() + " " + e.getMessage(), e);
        } catch (InterruptedException e) {
            throw new RuntimeException(Thread.currentThread().getName() + " " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isRetryable(Throwable t) {
        while (t != null) {
            if (t instanceof TsurugiDiagnosticCodeProvider) {
                return isRetyiableTsurugiException((TsurugiDiagnosticCodeProvider) t);
            }
            t = t.getCause();
        }
        return false;
    }

    protected boolean isRetyiableTsurugiException(TsurugiDiagnosticCodeProvider e) {
        var exceptionUtil = TsurugiExceptionUtil.getInstance();
        if (exceptionUtil.isSerializationFailure(e)) {
            return true;
        }
        if (exceptionUtil.isConflictOnWritePreserve(e)) {
            return true;
        }
        if (exceptionUtil.isInactiveTransaction(e)) {
            return true;
        }
        return false;
    }

    @Override
    public void commit(Runnable listener) {
        if (listener != null) {
            var transaction = getCurrentTransaction();
            transaction.addEventListener(new TsurugiTransactionEventListener() {
                @Override
                public void commitEnd(TsurugiTransaction transaction, TgCommitType commitType, Throwable occurred) {
                    if (occurred == null) {
                        listener.run();
                    }
                }
            });
        }
    }

    @Override
    public void rollback(Runnable listener) {
        var transaction = getCurrentTransaction();
        if (listener != null) {
            transaction.addEventListener(new TsurugiTransactionEventListener() {
                @Override
                public void rollbackEnd(TsurugiTransaction transaction, Throwable occurred) {
                    if (occurred == null) {
                        listener.run();
                    }
                }
            });
        }
        try {
            transaction.rollback();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(Thread.currentThread().getName() + " " + e.getMessage(), e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    @Override
    public void close() {
        closed.set(true);
        closeConnectionAll();
    }

    @Override
    public void closeConnection() {
        if (isMultiSession()) {
            closeConnectionAll();
        }
    }

    private void closeConnectionAll() {
        LOG.info("{} all session close start. sessionList.size={}", purpose, sessionList.size());
        RuntimeException re = null;
        for (var session : sessionList) {
            LOG.debug("close session {}", session);
            try {
                session.close();
            } catch (IOException e) {
                if (re == null) {
                    re = new UncheckedIOException(e.getMessage(), e);
                } else {
                    re.addSuppressed(e);
                }
            } catch (InterruptedException e) {
                if (re == null) {
                    re = new RuntimeException(e);
                } else {
                    re.addSuppressed(e);
                }
            }
        }

        if (re != null) {
            LOG.error("{} all session close error. message={}", purpose, re.getMessage());
            throw re;
        }

        sessionList.clear();
        LOG.info("{} all session close end.   sessionList.size={}", purpose, sessionList.size());
    }
}
