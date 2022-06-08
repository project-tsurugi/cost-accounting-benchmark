package com.example.nedo.online.task;

import java.util.List;

import com.example.nedo.db.CostBenchDbManager;
import com.example.nedo.db.doma2.dao.FactoryMasterDao;
import com.example.nedo.db.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.db.doma2.dao.ItemMasterDao;
import com.example.nedo.db.doma2.dao.ResultTableDao;
import com.example.nedo.db.doma2.entity.FactoryMaster;
import com.example.nedo.db.doma2.entity.ItemMaster;
import com.example.nedo.db.doma2.entity.ResultTable;
import com.example.nedo.init.InitialData;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * 重量の照会
 */
public class BenchOnlineShowWeightTask extends BenchOnlineTask {

    private static final TgTmSetting TX_MAIN = TgTmSetting.of( //
            TgTxOption.ofOCC(), //
            TgTxOption.ofRTX());

    public BenchOnlineShowWeightTask() {
        super("show-weight");
    }

    @Override
    protected boolean execute1() {
        return dbManager.execute(TX_MAIN, () -> {
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
