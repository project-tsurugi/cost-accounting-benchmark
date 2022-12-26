package com.tsurugidb.benchmark.costaccounting.db;

import java.io.Closeable;
import java.util.function.Supplier;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIceaxe;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;
import com.tsurugidb.benchmark.costaccounting.db.tsubakuro.CostBenchDbManagerTsubakuro;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtil;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public abstract class CostBenchDbManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManager.class);

    private final String name;
    private final boolean isTsurugi;
    private MeasurementMasterDao measurementMasterDao;
    private FactoryMasterDao factoryMasterDao;
    private ItemMasterDao itemMasterDao;
    private ItemConstructionMasterDao itemConstructionMasterDao;
    private ItemManufacturingMasterDao itemManufacturingMasterDao;
    private CostMasterDao costMasterDao;
    private ResultTableDao resultTableDao;

    private boolean isSingleTransaction = false;

    public static CostBenchDbManager createInstance(int type, IsolationLevel isolationLevel, boolean isMultiSession) {
        CostBenchDbManager manager;
        {
            switch (type) {
            default:
                manager = new CostBenchDbManagerJdbc(isolationLevel);
                break;
            case 2:
                manager = new CostBenchDbManagerIceaxe(isMultiSession);
                break;
            case 3:
                manager = new CostBenchDbManagerTsubakuro();
                break;
            }
        }
        LOG.info("using {}", manager.getClass().getSimpleName());

        MeasurementUtil.initialize(manager.getMeasurementMasterDao());

        return manager;
    }

    public CostBenchDbManager(String name, boolean isTsurugi) {
        this.name = name;
        this.isTsurugi = isTsurugi;
    }

    public String getName() {
        return this.name;
    }

    public boolean isTsurugi() {
        return this.isTsurugi;
    }

    // DAO

    public synchronized MeasurementMasterDao getMeasurementMasterDao() {
        if (measurementMasterDao == null) {
            this.measurementMasterDao = newMeasurementMasterDao();
        }
        return measurementMasterDao;
    }

    protected abstract MeasurementMasterDao newMeasurementMasterDao();

    public synchronized FactoryMasterDao getFactoryMasterDao() {
        if (factoryMasterDao == null) {
            this.factoryMasterDao = newFactoryMasterDao();
        }
        return factoryMasterDao;
    }

    protected abstract FactoryMasterDao newFactoryMasterDao();

    public synchronized ItemMasterDao getItemMasterDao() {
        if (itemMasterDao == null) {
            this.itemMasterDao = newItemMasterDao();
        }
        return itemMasterDao;
    }

    protected abstract ItemMasterDao newItemMasterDao();

    public synchronized ItemConstructionMasterDao getItemConstructionMasterDao() {
        if (itemConstructionMasterDao == null) {
            this.itemConstructionMasterDao = newItemConstructionMasterDao();
        }
        return itemConstructionMasterDao;
    }

    protected abstract ItemConstructionMasterDao newItemConstructionMasterDao();

    public synchronized ItemManufacturingMasterDao getItemManufacturingMasterDao() {
        if (itemManufacturingMasterDao == null) {
            this.itemManufacturingMasterDao = newItemManufacturingMasterDao();
        }
        return itemManufacturingMasterDao;
    }

    protected abstract ItemManufacturingMasterDao newItemManufacturingMasterDao();

    public synchronized CostMasterDao getCostMasterDao() {
        if (costMasterDao == null) {
            this.costMasterDao = newCostMasterDao();
        }
        return costMasterDao;
    }

    protected abstract CostMasterDao newCostMasterDao();

    public synchronized ResultTableDao getResultTableDao() {
        if (resultTableDao == null) {
            this.resultTableDao = newResultTableDao();
        }
        return resultTableDao;
    }

    protected abstract ResultTableDao newResultTableDao();

    // execute

    public void setSingleTransaction(boolean isSingleTransaction) {
        this.isSingleTransaction = isSingleTransaction;
    }

    public boolean isSingleTransaction() {
        return this.isSingleTransaction;
    }

    public abstract void executeDdl(String... sqls);

    public abstract void execute(TgTmSetting setting, Runnable runnable);

    public abstract <T> T execute(TgTmSetting setting, Supplier<T> supplier);

    public abstract boolean isRetriable(Throwable t);

    public final void commit() {
        commit(null);
    }

    public abstract void commit(Runnable listener);

    public final void rollback() {
        rollback(null);
    }

    public abstract void rollback(Runnable listener);

    @Override
    public abstract void close();
}
