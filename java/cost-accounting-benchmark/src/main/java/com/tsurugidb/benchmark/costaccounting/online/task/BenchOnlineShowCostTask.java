package com.tsurugidb.benchmark.costaccounting.online.task;

import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.FactoryMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.iceaxe.transaction.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.TgTxOption;

/**
 * 原価の照会
 */
public class BenchOnlineShowCostTask extends BenchOnlineTask {

    private static final TgTmSetting TX_MAIN = TgTmSetting.of( //
            TgTxOption.ofOCC(), //
            TgTxOption.ofRTX());

    public BenchOnlineShowCostTask() {
        super("show-cost");
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
        try (Stream<ResultTable> stream = resultTableDao.selectCost(factoryId, date)) {
            stream.forEach(result -> {
                ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
                ItemMaster item = itemMasterDao.selectById(result.getRIId(), date);
                console("factory=%s, product=%s, total=%s, quantity=%s, cost=%s", factory.getFName(), item.getIName(), result.getRTotalManufacturingCost(), result.getRManufacturingQuantity(),
                        result.getRManufacturingCost());
            });
        }
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineShowCostTask task = new BenchOnlineShowCostTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
