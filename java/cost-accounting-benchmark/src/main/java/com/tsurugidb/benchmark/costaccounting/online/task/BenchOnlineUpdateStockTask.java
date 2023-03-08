package com.tsurugidb.benchmark.costaccounting.online.task;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.StockTable;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtil;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 在庫の更新
 */
public class BenchOnlineUpdateStockTask extends BenchOnlineTask {

    private final TgTmSetting settingMain;

    public BenchOnlineUpdateStockTask() {
        super("update-stock");
        this.settingMain = getSetting(() -> TgTxOption.ofLTX(StockTableDao.TABLE_NAME));
    }

    @Override
    protected boolean execute1() {
        return dbManager.execute(settingMain, () -> {
            var dao = dbManager.getStockTableDao();
            dao.deleteByDate(date);

            executeInsert();
            return true;
        });
    }

    protected void executeInsert() {
        var dao = dbManager.getStockTableDao();
        var costMasterDao = dbManager.getCostMasterDao();

        try (var stream = costMasterDao.selectOrderIid()) {
            StockTable[] entityCache = { null };

            stream.forEach(cost -> {
                if (entityCache[0] != null) {
                    var entity = entityCache[0];
                    if (entity.getSIId().intValue() != cost.getCIId().intValue()) { // key break
                        dao.insert(entity);
                        entityCache[0] = null;
                    }
                }

                var targetUnit = getUnit(cost.getCStockUnit());
                var quantity = MeasurementUtil.convertUnit(cost.getCStockQuantity(), cost.getCStockUnit(), targetUnit);
                var amount = cost.getCStockAmount();
                if (entityCache[0] == null) {
                    var entity = new StockTable();
                    entity.setSDate(date);
                    entity.setSIId(cost.getCIId());
                    entity.setSStockUnit(targetUnit);
                    entity.setSStockQuantity(quantity);
                    entity.setSStockAmount(amount);
                    entityCache[0] = entity;
                } else {
                    var entity = entityCache[0];
                    entity.setSStockQuantity(entity.getSStockQuantity().add(quantity));
                    entity.setSStockAmount(entity.getSStockAmount().add(amount));
                }
            });

            if (entityCache[0] != null) {
                var entity = entityCache[0];
                dao.insert(entity);
            }
        }
    }

    private String getUnit(String unit) {
        var type = MeasurementUtil.getType(unit);
        switch (type) {
        case LENGTH:
            return "m";
        case CAPACITY:
            return "L";
        case WEIGHT:
            return "kg";
        default:
            return unit;
        }
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
