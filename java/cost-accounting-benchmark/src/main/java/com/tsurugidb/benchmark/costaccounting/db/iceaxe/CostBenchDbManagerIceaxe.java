package com.tsurugidb.benchmark.costaccounting.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
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
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.CostMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.FactoryMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.IceaxeDao;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemConstructionMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemManufacturingMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.MeasurementMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ResultTableDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.exception.TsurugiDiagnosticCodeProvider;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

public class CostBenchDbManagerIceaxe extends CostBenchDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManagerIceaxe.class);

    private final TsurugiConnector connector;
    private final TgSessionInfo sessionInfo;
    private final TsurugiTransactionManager singleTransactionManager;
    private final List<TsurugiSession> sessionList = new CopyOnWriteArrayList<>();
    private final ThreadLocal<TsurugiTransactionManager> transactionManagerThreadLocal = new ThreadLocal<>() {
        @Override
        protected TsurugiTransactionManager initialValue() {
            try {
                var session = connector.createSession(sessionInfo);
                sessionList.add(session);
                LOG.debug("create session. sessionList.size={}", sessionList.size());
                var tm = session.createTransactionManager();
                tm.addEventListener(counter);
                return tm;
            } catch (IOException e) {
                throw new UncheckedIOException(e.getMessage(), e);
            }
        }
    };

    private TsurugiTransaction singleTransaction;
    private final ThreadLocal<TsurugiTransaction> transactionThreadLocal = new ThreadLocal<>();

    public CostBenchDbManagerIceaxe(boolean isMultiSession) {
        super("ICEAXE", true);
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        this.connector = TsurugiConnector.createConnector(endpoint);
        try {
            var credential = new UsernamePasswordCredential(BenchConst.tsurugiUser(), BenchConst.tsurugiPassword());
            this.sessionInfo = TgSessionInfo.of(credential);
            if (!isMultiSession) {
                var session = connector.createSession(sessionInfo);
                var tm = session.createTransactionManager();
                tm.addEventListener(counter);
                this.singleTransactionManager = tm;
                sessionList.add(session);
            } else {
                this.singleTransactionManager = null;
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
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
        } catch (TsurugiTransactionIOException e) {
            // FIXME コミット時の一意制約違反の判定方法
            var c = e.getCause();
            if (c instanceof TsurugiTransactionException) {
                var t = (TsurugiTransactionException) c;
                if (t.getDiagnosticCode() == SqlServiceCode.ERR_ABORTED_RETRYABLE) {
                    String message = t.getMessage();
                    if (message.contains("Status=ERR_VALIDATION") && message.contains("reason=KVS_INSERT")) {
                        throw new UniqueConstraintException(e);
                    }
                }
            }
            if (c instanceof UniqueConstraintException) {
                throw (UniqueConstraintException) c;
            }
            throw new UncheckedIOException(Thread.currentThread().getName() + " " + e.getMessage(), e);
        } catch (IOException e) {
            throw new UncheckedIOException(Thread.currentThread().getName() + " " + e.getMessage(), e);
        }
    }

    @Override
    public boolean isRetriable(Throwable t) {
        while (t != null) {
            if (t instanceof TsurugiDiagnosticCodeProvider) {
                return isRetyiableTsurugiException((TsurugiDiagnosticCodeProvider) t);
            }
            t = t.getCause();
        }
        return false;
    }

    protected boolean isRetyiableTsurugiException(TsurugiDiagnosticCodeProvider e) {
        var code = e.getDiagnosticCode();
        if (code == SqlServiceCode.ERR_ABORTED_RETRYABLE) {
            return true;
        }
        if (code == SqlServiceCode.ERR_CONFLICT_ON_WRITE_PRESERVE) {
            return true;
        }
        if (code == SqlServiceCode.ERR_INACTIVE_TRANSACTION) {
            return true;
        }
        return false;
    }

    @Override
    public void commit(Runnable listener) {
        if (listener != null) {
            var transaction = getCurrentTransaction();
            transaction.addCommitListener(tx -> listener.run());
        }
    }

    @Override
    public void rollback(Runnable listener) {
        var transaction = getCurrentTransaction();
        if (listener != null) {
            transaction.addRollbackListener(tx -> listener.run());
        }
        try {
            transaction.rollback();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (TsurugiTransactionException e) {
            throw new TsurugiTransactionRuntimeException(e);
        }
    }

    @Override
    public void close() {
        UncheckedIOException u = null;
        for (var session : sessionList) {
            LOG.debug("close session {}", session);
            try {
                session.close();
            } catch (IOException e) {
                if (u == null) {
                    u = new UncheckedIOException(e.getMessage(), e);
                } else {
                    u.addSuppressed(e);
                }
            }
        }

        if (u != null) {
            throw u;
        }
    }
}
