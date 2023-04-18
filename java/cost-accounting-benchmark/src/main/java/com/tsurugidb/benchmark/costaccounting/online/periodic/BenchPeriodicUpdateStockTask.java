package com.tsurugidb.benchmark.costaccounting.online.periodic;

import java.time.LocalTime;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistory;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 在庫履歴の追加
 */
public class BenchPeriodicUpdateStockTask extends BenchPeriodicTask {
    public static final String TASK_NAME = "update-stock";

    private final TgTmSetting settingMain;

    public BenchPeriodicUpdateStockTask() {
        super(TASK_NAME);
        this.settingMain = getSetting(() -> TgTxOption.ofLTX(StockHistoryDao.TABLE_NAME));
    }

    @Override
    protected boolean execute1() {
        return dbManager.execute(settingMain, () -> {
            executeCopy();

            return true;
        });
    }

    protected void executeCopy() {
        var time = LocalTime.now();

        var dao = dbManager.getStockHistoryDao();

        if (!BenchConst.WORKAROUND) {
            // TODO select-insert
            throw new AssertionError("implemtens select-insert");
        }

        var costMasterDao = dbManager.getCostMasterDao();
        try (var stream = costMasterDao.selectAll()) {
            stream.map(cost -> {
                var entity = new StockHistory();
                entity.setSDate(date);
                entity.setSFId(cost.getCFId());
                entity.setSIId(cost.getCIId());
                entity.setSTime(time);
                entity.setSStockUnit(cost.getCStockUnit());
                entity.setSStockQuantity(cost.getCStockQuantity());
                entity.setSStockAmount(cost.getCStockAmount());
                return entity;
            }).forEach(dao::insert);
        }
    }

    // for test
    public static void main(String... args) {
        var task = new BenchPeriodicUpdateStockTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
