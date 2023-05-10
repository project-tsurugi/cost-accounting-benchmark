package com.tsurugidb.benchmark.costaccounting.online.task;

import java.util.stream.Stream;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.FactoryMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 原価の照会
 */
public class BenchOnlineShowCostTask extends BenchOnlineTask {
    public static final String TASK_NAME = "show-cost";

    private TgTmSetting settingMain;

    public BenchOnlineShowCostTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting(OnlineConfig config) {
        this.settingMain = config.getSetting(LOG, this, () -> TgTxOption.ofRTX());
        setTxOptionDescription(settingMain);
    }

    @Override
    protected boolean execute1() {
        dbManager.execute(settingMain, () -> {
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
        BenchOnlineShowCostTask task = new BenchOnlineShowCostTask(0);

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
