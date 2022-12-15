package com.tsurugidb.benchmark.costaccounting.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
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
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionIOException;
import com.tsurugidb.iceaxe.transaction.exception.TsurugiTransactionRuntimeException;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.manager.TsurugiTransactionManager;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;

public class CostBenchDbManagerIceaxe extends CostBenchDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManagerIceaxe.class);

    private final TsurugiSession session;
    private final TsurugiTransactionManager transactionManager;

    private TsurugiTransaction singleTransaction;
    private final ThreadLocal<TsurugiTransaction> transactionThreadLocal = new ThreadLocal<>();

    public CostBenchDbManagerIceaxe() {
        super("ICEAXE");
        var endpoint = BenchConst.tsurugiEndpoint();
        var connector = TsurugiConnector.createConnector(endpoint);
        try {
            var credential = new UsernamePasswordCredential(BenchConst.tsurugiUser(), BenchConst.tsurugiPassword());
            var info = TgSessionInfo.of(credential);
            this.session = connector.createSession(info);
            this.transactionManager = session.createTransactionManager();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TsurugiSession getSession() {
        return this.session;
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

                    try (var ps = createPreparedStatement(sql)) {
                        ps.executeAndGetCount(transactionManager, setting);
                    } catch (IOException e) {
                        LOG.info("ddl={}", sql.trim());
                        if (sql.equals(sqls[sqls.length - 1])) {
                            throw new UncheckedIOException(e);
                        }
                        LOG.warn("execption={}", e.getMessage());
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
        try {
            transactionManager.execute(setting, transaction -> {
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
            throw new UncheckedIOException(Thread.currentThread().getName(), e);
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        try {
            return transactionManager.execute(setting, transaction -> {
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
            var code = e.getDiagnosticCode();
            // FIXME コミット時の一意制約違反の判定方法
            if (code == SqlServiceCode.ERR_ALREADY_EXISTS || code == SqlServiceCode.ERR_ABORTED) {
                throw new UniqueConstraintException(e);
            }
            throw new UncheckedIOException(Thread.currentThread().getName(), e);
        } catch (IOException e) {
            throw new UncheckedIOException(Thread.currentThread().getName(), e);
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
        try {
            session.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
