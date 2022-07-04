package com.tsurugidb.benchmark.costaccounting.db.doma2;

import java.util.function.Supplier;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.doma2.config.AppConfig;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.CostMasterDaoImpl;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.FactoryMasterDaoImpl;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemConstructionMasterDaoImpl;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemMasterDaoImpl;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.MeasurementMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.MeasurementMasterDaoImpl;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ResultTableDaoImpl;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;

public class CostBenchDbManagerDoma2 extends CostBenchDbManager {

    private final TransactionManager tm;

    public CostBenchDbManagerDoma2() {
        this.tm = AppConfig.singleton().getTransactionManager();
    }

    @Override
    protected MeasurementMasterDao newMeasurementMasterDao() {
        return new MeasurementMasterDaoImpl();
    }

    @Override
    protected FactoryMasterDao newFactoryMasterDao() {
        return new FactoryMasterDaoImpl();
    }

    @Override
    protected ItemMasterDao newItemMasterDao() {
        return new ItemMasterDaoImpl();
    }

    @Override
    protected ItemConstructionMasterDao newItemConstructionMasterDao() {
        return new ItemConstructionMasterDaoImpl();
    }

    @Override
    protected ItemManufacturingMasterDao newItemManufacturingMasterDao() {
        return new ItemManufacturingMasterDaoImpl();
    }

    @Override
    protected CostMasterDao newCostMasterDao() {
        return new CostMasterDaoImpl();
    }

    @Override
    protected ResultTableDao newResultTableDao() {
        return new ResultTableDaoImpl();
    }

    @Override
    public void execute(TgTmSetting setting, Runnable runnable) {
        tm.required(runnable);
    }

    @Override
    public <T> T execute(TgTmSetting setting, Supplier<T> supplier) {
        return tm.required(supplier);
    }

    @Override
    public void commit() {
        // do nothing
    }

    @Override
    public void rollback() {
        tm.setRollbackOnly();
    }

    @Override
    public void close() {
        // do nothing
    }
}
