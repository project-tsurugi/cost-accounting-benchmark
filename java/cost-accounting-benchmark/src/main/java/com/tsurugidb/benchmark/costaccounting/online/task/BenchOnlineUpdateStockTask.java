package com.tsurugidb.benchmark.costaccounting.online.task;

import java.math.BigDecimal;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockTable;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 在庫の更新
 */
public class BenchOnlineUpdateStockTask extends BenchOnlineTask {
    public static final String TASK_NAME = "update-stock";

    private final TgTmSetting settingMain;

    public BenchOnlineUpdateStockTask() {
        super(TASK_NAME);
        this.settingMain = getSetting(() -> TgTxOption.ofLTX(StockTableDao.TABLE_NAME));
    }

    @Override
    protected boolean execute1() {
        return dbManager.execute(settingMain, () -> {
            var dao = dbManager.getStockTableDao();
            dao.deleteByDateFactory(date, factoryId);

            executeInsert();
            return true;
        });
    }

    protected void executeInsert() {
        var dao = dbManager.getStockTableDao();

        var costMasterDao = dbManager.getCostMasterDao();
        BigDecimal amount = costMasterDao.selectSumByFactory(factoryId);

        var entity = new StockTable();
        entity.setSDate(date);
        entity.setSFId(factoryId);
        entity.setSStockAmount(amount);

        dao.insert(entity);
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineUpdateStockTask task = new BenchOnlineUpdateStockTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
