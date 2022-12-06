package com.tsurugidb.benchmark.costaccounting.db.tsubakuro;

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
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.CostMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.FactoryMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.ItemConstructionMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.ItemManufacturingMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.ItemMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.MeasurementMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.ResultTableDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.TsubakuroDao;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.SqlServiceCode;
import com.tsurugidb.tsubakuro.sql.Transaction;

public class CostBenchDbManagerTsubakuro extends CostBenchDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManagerTsubakuro.class);

    private final Session session;
    private final SqlClient sqlClient;

    private Transaction singleTransaction;
    private final ThreadLocal<Transaction> transactionThreadLocal = new ThreadLocal<>();

    public CostBenchDbManagerTsubakuro() {
        var endpoint = BenchConst.tsurugiEndpoint();
        try {
            var credential = new UsernamePasswordCredential(BenchConst.tsurugiUser(), BenchConst.tsurugiPassword());
            this.session = SessionBuilder.connect(endpoint).withCredential(credential).create();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        this.sqlClient = SqlClient.attach(session);
    }

    public SqlClient getSqlClient() {
        return this.sqlClient;
    }

    private void setCurrentTransaction(Transaction transaction) {
        if (isSingleTransaction()) {
            this.singleTransaction = transaction;
            return;
        }
        transactionThreadLocal.set(transaction);
    }

    public Transaction getCurrentTransaction() {
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
        return new MeasurementMasterDaoTsubakuro(this);
    }

    @Override
    protected FactoryMasterDao newFactoryMasterDao() {
        return new FactoryMasterDaoTsubakuro(this);
    }

    @Override
    protected ItemMasterDao newItemMasterDao() {
        return new ItemMasterDaoTsubakuro(this);
    }

    @Override
    protected ItemConstructionMasterDao newItemConstructionMasterDao() {
        return new ItemConstructionMasterDaoTsubakuro(this);
    }

    @Override
    protected ItemManufacturingMasterDao newItemManufacturingMasterDao() {
        return new ItemManufacturingMasterDaoTsubakuro(this);
    }

    @Override
    protected CostMasterDao newCostMasterDao() {
        return new CostMasterDaoTsubakuro(this);
    }

    @Override
    protected ResultTableDao newResultTableDao() {
        return new ResultTableDaoTsubakuro(this);
    }

    @Override
    public void executeDdl(String... sqls) {
        var dao = new TsubakuroDao<Object>(this, null, null, null) {
            public void executeDdl() {
                var setting = TgTmSetting.of(TgTxOption.ofOCC());
                for (var sql : sqls) {
                    if (sql.startsWith("drop table")) {
                        int n = sql.lastIndexOf(" ");
                        String tableName = sql.substring(n + 1).trim();
                        try {
                            sqlClient.getTableMetadata(tableName).await();
                        } catch (ServerException e) {
                            if (e.getDiagnosticCode() == SqlServiceCode.ERR_NOT_FOUND) {
                                continue;
                            }
                            throw new RuntimeException(e);
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        } catch (InterruptedException e) {
                            throw new RuntimeException(e);
                        }
                    }

                    execute(setting, () -> {
                        var transaction = getTransaction();
                        try {
                            transaction.executeStatement(sql);
                        } catch (IOException e) {
                            LOG.info("ddl={}", sql.trim());
                            if (sql.equals(sqls[sqls.length - 1])) {
                                throw new UncheckedIOException(e);
                            }
                            LOG.warn("execption={}", e.getMessage());
                        }
                    });
                }
            }
        };
        dao.executeDdl();
    }

    @Override
    public void execute(TgTmSetting setting, Runnable runnable) {
        var option = setting.getTransactionOption(0, null).getOption().toLowTransactionOption();
        try {
            var transaction = sqlClient.createTransaction(option).await();
            setCurrentTransaction(transaction);
            try {
                runnable.run();
                commit();
            } finally {
                removeCurrentTransaction();
            }
        } catch (IOException e) {
            rollback();
            throw new UncheckedIOException(Thread.currentThread().getName(), e);
        } catch (ServerException e) {
            rollback();
            // TODO abort retry
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        var option = setting.getTransactionOption(0, null).getOption().toLowTransactionOption();
        try {
            var transaction = sqlClient.createTransaction(option).await();
            setCurrentTransaction(transaction);
            try {
                T result = supplier.get();
                commit();
                return result;
            } finally {
                removeCurrentTransaction();
            }
        } catch (IOException e) {
            rollback();
            throw new UncheckedIOException(Thread.currentThread().getName(), e);
        } catch (ServerException e) {
            rollback();
            // TODO abort retry
            var code = e.getDiagnosticCode();
            // FIXME コミット時の一意制約違反の判定方法
            if (code == SqlServiceCode.ERR_ALREADY_EXISTS || code == SqlServiceCode.ERR_ABORTED) {
                throw new UniqueConstraintException(e);
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            rollback();
            throw new RuntimeException(e);
        }
    }

    @Override
    public void commit(Runnable listener) {
        var transaction = getCurrentTransaction();
        try {
            transaction.commit().await();
        } catch (IOException e) {
            throw new UncheckedIOException(Thread.currentThread().getName(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (listener != null) {
            listener.run();
        }
    }

    @Override
    public void rollback(Runnable listener) {
        var transaction = getCurrentTransaction();
        try {
            transaction.rollback().await();
        } catch (IOException e) {
            throw new UncheckedIOException(Thread.currentThread().getName(), e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        if (listener != null) {
            listener.run();
        }
    }

    @Override
    public void close() {
        try (session; sqlClient) {
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}