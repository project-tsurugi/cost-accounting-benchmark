package com.example.nedo.online.task;

import java.util.stream.Stream;

import com.example.nedo.db.CostBenchDbManager;
import com.example.nedo.db.doma2.dao.FactoryMasterDao;
import com.example.nedo.db.doma2.dao.ItemMasterDao;
import com.example.nedo.db.doma2.dao.ResultTableDao;
import com.example.nedo.db.doma2.entity.FactoryMaster;
import com.example.nedo.db.doma2.entity.ItemMaster;
import com.example.nedo.db.doma2.entity.ResultTable;
import com.example.nedo.init.InitialData;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * 所要量の照会
 */
public class BenchOnlineShowQuantityTask extends BenchOnlineTask {

    private static final TgTmSetting TX_MAIN = TgTmSetting.of( //
            TgTxOption.ofOCC(), //
            TgTxOption.ofRTX());

    public BenchOnlineShowQuantityTask() {
        super("show-quantity");
    }

    @Override
    protected boolean execute1() {
        dbManager.execute(TX_MAIN, () -> {
            executeMain();
        });
        return true;
    }

    protected void executeMain() {
        FactoryMasterDao factoryMasterDao = dbManager.getFactoryMasterDao();
        FactoryMaster factory = factoryMasterDao.selectById(factoryId);

        ResultTableDao resultTableDao = dbManager.getResultTableDao();
        try (Stream<ResultTable> stream = resultTableDao.selectRequiredQuantity(factoryId, date)) {
            stream.forEach(result -> {
                ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
                ItemMaster item = itemMasterDao.selectById(result.getRIId(), date);
                console("factory=%s, item=%s, required_quantity=%s %s", factory.getFName(), item.getIName(), result.getRRequiredQuantity(), result.getRRequiredQuantityUnit());
            });
        }
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineShowQuantityTask task = new BenchOnlineShowQuantityTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
