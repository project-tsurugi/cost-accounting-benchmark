package com.tsurugidb.benchmark.costaccounting.online.task;

import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.FactoryMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 重量の照会
 */
public class BenchOnlineShowWeightTask extends BenchOnlineTask {

    private final TgTmSetting settingMain;

    public BenchOnlineShowWeightTask() {
        super("show-weight");
        this.settingMain = getSetting(() -> TgTxOption.ofRTX());
    }

    @Override
    protected boolean execute1() {
        return dbManager.execute(settingMain, () -> {
            int productId = selectRandomItemId();
            if (productId < 0) {
                return false;
            }
            logTarget("factory=%d, date=%s, product=%d", factoryId, date, productId);
            executeMain(productId);
            return true;
        });
    }

    protected int selectRandomItemId() {
        ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
        List<Integer> list = itemManufacturingMasterDao.selectIdByFactory(factoryId, date);
        if (list.isEmpty()) {
            return -1;
        }
        int i = random.nextInt(list.size());
        return list.get(i);
    }

    protected void executeMain(int productId) {
        ResultTableDao resultTableDao = dbManager.getResultTableDao();
        List<ResultTable> list = resultTableDao.selectByProductId(factoryId, date, productId);

        FactoryMasterDao factoryMasterDao = dbManager.getFactoryMasterDao();
        FactoryMaster factory = factoryMasterDao.selectById(factoryId);

        ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
        ItemMaster product = itemMasterDao.selectById(productId, date);
        for (ResultTable result : list) {
            ItemMaster item = itemMasterDao.selectById(result.getRIId(), date);
            console("factory=%s, product=%s, parent=%d, item=%s, weight=%s %s, ratio=%.3f", factory.getFName(), product.getIName(), result.getRParentIId(), item.getIName(), result.getRWeightTotal(),
                    result.getRWeightTotalUnit(), result.getRWeightRatio());
        }
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineShowWeightTask task = new BenchOnlineShowWeightTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
