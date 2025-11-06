/*
 * Copyright 2023-2025 Project Tsurugi.
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
package com.tsurugidb.benchmark.costaccounting.db.tsubakuro;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.BenchDbCounter.CounterName;
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
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.CostMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.FactoryMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.ItemConstructionMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.ItemManufacturingMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.ItemMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.MeasurementMasterDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.ResultTableDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.StockHistoryDaoTsubakuro;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.dao.TsubakuroDao;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;
import com.tsurugidb.tsubakuro.channel.common.connection.UsernamePasswordCredential;
import com.tsurugidb.tsubakuro.common.Session;
import com.tsurugidb.tsubakuro.common.SessionBuilder;
import com.tsurugidb.tsubakuro.exception.ServerException;
import com.tsurugidb.tsubakuro.sql.SqlClient;
import com.tsurugidb.tsubakuro.sql.Transaction;
import com.tsurugidb.tsubakuro.sql.exception.CcException;
import com.tsurugidb.tsubakuro.sql.exception.TargetNotFoundException;
import com.tsurugidb.tsubakuro.sql.exception.UniqueConstraintViolationException;

public class CostBenchDbManagerTsubakuro extends CostBenchDbManager {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManagerTsubakuro.class);

    private final Session session;
    private final SqlClient sqlClient;

    private Transaction singleTransaction;
    private final ThreadLocal<Transaction> transactionThreadLocal = new ThreadLocal<>();

    public CostBenchDbManagerTsubakuro(DbManagerPurpose purpose) {
        super("TSUBAKURO", true, purpose);
        var endpoint = BenchConst.tsurugiEndpoint();
        LOG.info("endpoint={}", endpoint);
        try {
            var credential = new UsernamePasswordCredential(BenchConst.tsurugiUser(), BenchConst.tsurugiPassword());
            this.session = SessionBuilder.connect(endpoint).withCredential(credential).withApplicationName("CostBenchDbManagerTsubakuro").create();
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
    protected StockHistoryDao newStockHistoryDao() {
        return new StockHistoryDaoTsubakuro(this);
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
                            if (e instanceof TargetNotFoundException) {
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
        try {
            var info = setting.getTransactionOptionSupplier().createExecuteInfo(0);
            var option = setting.getFirstTransactionOption(info).toLowTransactionOption();
            var transaction = sqlClient.createTransaction(option).await();
            setCurrentTransaction(transaction);

            counter.increment(setting, CounterName.BEGIN_TX);
            runnable.run();
            counter.increment(setting, CounterName.TRY_COMMIT);
            commit();
            counter.increment(setting, CounterName.SUCCESS);
        } catch (IOException e) {
            counter.increment(setting, CounterName.ABORTED);
            rollback();
            throw new UncheckedIOException(Thread.currentThread().getName(), e);
        } catch (ServerException e) {
            counter.increment(setting, CounterName.ABORTED);
            rollback();
            // TODO abort retry
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            counter.increment(setting, CounterName.ABORTED);
            rollback();
            throw new RuntimeException(e);
        } catch (Throwable e) {
            counter.increment(setting, CounterName.ABORTED);
            rollback();
            throw e;
        } finally {
            removeCurrentTransaction();
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        try {
            var info = setting.getTransactionOptionSupplier().createExecuteInfo(0);
            var option = setting.getFirstTransactionOption(info).toLowTransactionOption();
            var transaction = sqlClient.createTransaction(option).await();
            setCurrentTransaction(transaction);

            counter.increment(setting, CounterName.BEGIN_TX);
            T result = supplier.get();
            counter.increment(setting, CounterName.TRY_COMMIT);
            commit();
            counter.increment(setting, CounterName.SUCCESS);
            return result;
        } catch (IOException e) {
            counter.increment(setting, CounterName.ABORTED);
            rollback();
            throw new UncheckedIOException(Thread.currentThread().getName(), e);
        } catch (ServerException e) {
            counter.increment(setting, CounterName.ABORTED);
            rollback();
            // TODO abort retry
            // FIXME コミット時の一意制約違反の判定方法
            if (e instanceof UniqueConstraintViolationException) {
                throw new UniqueConstraintException(e);
            }
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            counter.increment(setting, CounterName.ABORTED);
            rollback();
            throw new RuntimeException(e);
        } catch (Throwable e) {
            counter.increment(setting, CounterName.ABORTED);
            rollback();
            throw e;
        } finally {
            removeCurrentTransaction();
        }
    }

    @Override
    public boolean isRetryable(Throwable t) {
        while (t != null) {
            if (t instanceof ServerException) {
                return isRetyiableTsurugiException((ServerException) t);
            }
            t = t.getCause();
        }
        return false;
    }

    protected boolean isRetyiableTsurugiException(ServerException e) {
        return e instanceof CcException;
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
            // close only
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (ServerException | InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void closeConnection() {
        // do nothing
    }
}
