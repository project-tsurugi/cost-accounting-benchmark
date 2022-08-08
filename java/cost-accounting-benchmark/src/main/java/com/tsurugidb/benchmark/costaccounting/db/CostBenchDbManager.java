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
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtil;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class CostBenchDbManager implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(CostBenchDbManager.class);

    private MeasurementMasterDao measurementMasterDao;
    private FactoryMasterDao factoryMasterDao;
    private ItemMasterDao itemMasterDao;
    private ItemConstructionMasterDao itemConstructionMasterDao;
    private ItemManufacturingMasterDao itemManufacturingMasterDao;
    private CostMasterDao costMasterDao;
    private ResultTableDao resultTableDao;

    public static CostBenchDbManager createInstance(int type) {
        CostBenchDbManager manager;
        {
            switch (type) {
            default:
                manager = new CostBenchDbManagerJdbc();
                break;
            case 2:
                manager = new CostBenchDbManagerIceaxe();
                break;
            }
        }
        LOG.info("using {}", manager.getClass().getSimpleName());

        MeasurementUtil.initialize(manager.getMeasurementMasterDao());

        return manager;
    }

    public MeasurementMasterDao getMeasurementMasterDao() {
        if (measurementMasterDao == null) {
            this.measurementMasterDao = newMeasurementMasterDao();
        }
        return measurementMasterDao;
    }

    protected abstract MeasurementMasterDao newMeasurementMasterDao();

    public FactoryMasterDao getFactoryMasterDao() {
        if (factoryMasterDao == null) {
            this.factoryMasterDao = newFactoryMasterDao();
        }
        return factoryMasterDao;
    }

    protected abstract FactoryMasterDao newFactoryMasterDao();

    public ItemMasterDao getItemMasterDao() {
        if (itemMasterDao == null) {
            this.itemMasterDao = newItemMasterDao();
        }
        return itemMasterDao;
    }

    protected abstract ItemMasterDao newItemMasterDao();

    public ItemConstructionMasterDao getItemConstructionMasterDao() {
        if (itemConstructionMasterDao == null) {
            this.itemConstructionMasterDao = newItemConstructionMasterDao();
        }
        return itemConstructionMasterDao;
    }

    protected abstract ItemConstructionMasterDao newItemConstructionMasterDao();

    public ItemManufacturingMasterDao getItemManufacturingMasterDao() {
        if (itemManufacturingMasterDao == null) {
            this.itemManufacturingMasterDao = newItemManufacturingMasterDao();
        }
        return itemManufacturingMasterDao;
    }

    protected abstract ItemManufacturingMasterDao newItemManufacturingMasterDao();

    public CostMasterDao getCostMasterDao() {
        if (costMasterDao == null) {
            this.costMasterDao = newCostMasterDao();
        }
        return costMasterDao;
    }

    protected abstract CostMasterDao newCostMasterDao();

    public ResultTableDao getResultTableDao() {
        if (resultTableDao == null) {
            this.resultTableDao = newResultTableDao();
        }
        return resultTableDao;
    }

    protected abstract ResultTableDao newResultTableDao();

    // execute

    public abstract void executeDdl(String... sqls);

    public abstract void execute(TgTmSetting setting, Runnable runnable);

    public abstract <T> T execute(TgTmSetting setting, Supplier<T> supplier);

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
