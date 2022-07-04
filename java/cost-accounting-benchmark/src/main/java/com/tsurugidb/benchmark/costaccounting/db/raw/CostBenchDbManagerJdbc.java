package com.tsurugidb.benchmark.costaccounting.db.raw;

import java.sql.Connection;
import java.util.function.Supplier;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.raw.dao.CostMasterDaoRaw;
import com.tsurugidb.benchmark.costaccounting.db.raw.dao.FactoryMasterDaoRaw;
import com.tsurugidb.benchmark.costaccounting.db.raw.dao.ItemConstructionMasterDaoRaw;
import com.tsurugidb.benchmark.costaccounting.db.raw.dao.ItemManufacturingMasterDaoRaw;
import com.tsurugidb.benchmark.costaccounting.db.raw.dao.ItemMasterDaoRaw;
import com.tsurugidb.benchmark.costaccounting.db.raw.dao.MeasurementMasterDaoRaw;
import com.tsurugidb.benchmark.costaccounting.db.raw.dao.ResultTableDaoRaw;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public abstract class CostBenchDbManagerJdbc extends CostBenchDbManager {

    public CostBenchDbManagerJdbc() {
    }

    public abstract Connection getConnection();

    @Override
    protected MeasurementMasterDao newMeasurementMasterDao() {
        return new MeasurementMasterDaoRaw(this);
    }

    @Override
    protected FactoryMasterDao newFactoryMasterDao() {
        return new FactoryMasterDaoRaw(this);
    }

    @Override
    protected ItemMasterDao newItemMasterDao() {
        return new ItemMasterDaoRaw(this);
    }

    @Override
    protected ItemConstructionMasterDao newItemConstructionMasterDao() {
        return new ItemConstructionMasterDaoRaw(this);
    }

    @Override
    protected ItemManufacturingMasterDao newItemManufacturingMasterDao() {
        return new ItemManufacturingMasterDaoRaw(this);
    }

    @Override
    protected CostMasterDao newCostMasterDao() {
        return new CostMasterDaoRaw(this);
    }

    @Override
    protected ResultTableDao newResultTableDao() {
        return new ResultTableDaoRaw(this);
    }

    @Override
    public void execute(TgTmSetting setting, Runnable runnable) {
        try {
            runnable.run();
            commit();
        } catch (Throwable e) {
            try {
                rollback();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        try {
            T r = supplier.get();
            commit();
            return r;
        } catch (Throwable e) {
            try {
                rollback();
            } catch (Throwable t) {
                e.addSuppressed(t);
            }
            throw e;
        }
    }
}
