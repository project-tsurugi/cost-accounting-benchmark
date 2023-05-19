package com.tsurugidb.benchmark.costaccounting.time.table;

import java.math.BigDecimal;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class CostMasterTime extends TableTime {

    private CostMasterDao dao;

    public CostMasterTime() {
        super(CostMasterDao.TABLE_NAME);
    }

    @Override
    public void execute() {
        this.dao = dbManager.getCostMasterDao();
        clear();
        insert();
        selectRangeScan();
    }

    private void clear() {
        if (!BenchConst.timeCommandExecute(tableName, "clear")) {
            return;
        }

        var setting = TgTmSetting.of(TgTxOption.ofLTX(tableName));
        dbManager.execute(setting, () -> {
            dao.truncate();
        });
    }

    private void insert() {
        execute("insert", () -> {
            for (int f = sizeAdjustStart; f <= sizeAdjustEnd; f++) {
                for (int i = 0; i < size; i++) {
                    var entity = createCostMaster(f, i);
                    dao.insert(entity);
                }
            }
        });
    }

    private CostMaster createCostMaster(int factoryId, int i) {
        var entity = new CostMaster();
        entity.setCFId(factoryId);
        entity.setCIId(i);
        entity.setCStockUnit("g");
        entity.setCStockQuantity(BigDecimal.valueOf(100));
        entity.setCStockAmount(BigDecimal.valueOf(300));
        return entity;
    }

    private void selectRangeScan() {
        int fId = (sizeAdjustStart + sizeAdjustEnd) / 2;
        execute("select(range-scan)", 3, () -> {
            dao.selectIdByFactory(fId);
        });
    }
}
