package com.tsurugidb.benchmark.costaccounting.db.iceaxe;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.costaccounting.BenchConst;
import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.CostMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.FactoryMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemConstructionMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemManufacturingMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ItemMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.MeasurementMasterDaoIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao.ResultTableDaoIceaxe;
import com.tsurugidb.iceaxe.TsurugiConnector;
import com.tsurugidb.iceaxe.session.TgSessionInfo;
import com.tsurugidb.iceaxe.session.TsurugiSession;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TsurugiTransaction;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionException;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionManager;
import com.tsurugidb.iceaxe.transaction.TsurugiTransactionRuntimeException;

public class CostBenchDbManagerIxeaxe extends CostBenchDbManager {

    private final TsurugiSession session;
    private final TsurugiTransactionManager transactionManager;

    private final ThreadLocal<TsurugiTransaction> transactionThreadLocal = new ThreadLocal<>();

    public CostBenchDbManagerIxeaxe() {
        var endpoint = BenchConst.tsurugiEndpoint();
        var connector = TsurugiConnector.createConnector(endpoint);
        try {
            var info = TgSessionInfo.of(BenchConst.tsurugiUser(), BenchConst.tsurugiPassword());
            this.session = connector.createSession(info);
            this.transactionManager = session.createTransactionManager();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public TsurugiSession getSession() {
        return this.session;
    }

    public TsurugiTransaction getCurrentTransaction() {
        return transactionThreadLocal.get();
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
    public void execute(TgTmSetting setting, Runnable runnable) {
        try {
            transactionManager.execute(setting, transaction -> {
                transactionThreadLocal.set(transaction);
                try {
                    runnable.run();
                } finally {
                    transactionThreadLocal.remove();
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        try {
            return transactionManager.execute(setting, transaction -> {
                transactionThreadLocal.set(transaction);
                try {
                    return supplier.get();
                } finally {
                    transactionThreadLocal.remove();
                }
            });
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void commit() {
        // do nothing
    }

    @Override
    public void rollback() {
        var transaction = getCurrentTransaction();
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
