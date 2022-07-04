package com.tsurugidb.benchmark.costaccounting.db;

import java.io.Closeable;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.costaccounting.db.doma2.CostBenchDbManagerDoma2;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.iceaxe.CostBenchDbManagerIxeaxe;
import com.tsurugidb.benchmark.costaccounting.db.raw.CostBenchDbManagerJdbc1;
import com.tsurugidb.benchmark.costaccounting.db.raw.CostBenchDbManagerJdbc2;
import com.tsurugidb.benchmark.costaccounting.init.MeasurementUtil;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class CostBenchDbManager implements Closeable {

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
                manager = new CostBenchDbManagerDoma2();
                break;
            case 2:
                manager = new CostBenchDbManagerJdbc1();
                break;
            case 3:
                manager = new CostBenchDbManagerJdbc2();
                break;
            case 4:
                manager = new CostBenchDbManagerIxeaxe();
                break;
            }
        }
        System.out.println("using " + manager.getClass().getSimpleName());

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

    public abstract void execute(TgTmSetting setting, Runnable runnable);

    public abstract <T> T execute(TgTmSetting setting, Supplier<T> supplier);

    public abstract void commit();

    public abstract void rollback();

    @Override
    public abstract void close();
}
