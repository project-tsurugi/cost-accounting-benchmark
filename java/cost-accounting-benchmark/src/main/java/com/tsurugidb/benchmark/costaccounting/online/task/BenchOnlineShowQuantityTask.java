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
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 所要量の照会
 */
public class BenchOnlineShowQuantityTask extends BenchOnlineTask {
    public static final String TASK_NAME = "show-quantity";

    private TgTmSetting settingMain;

    public BenchOnlineShowQuantityTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting() {
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
        int[] count = { 0 };
        long start = System.nanoTime();
        try (Stream<ResultTable> stream = resultTableDao.selectRequiredQuantity(factoryId, date)) {
            stream.forEach(result -> {
                ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
                ItemMaster item = itemMasterDao.selectById(result.getRIId(), date);
                console("factory=%s, item=%s, required_quantity=%s %s", factory.getFName(), item.getIName(), result.getRRequiredQuantity(), result.getRRequiredQuantityUnit());
                count[0]++;
            });
        }
        long end = System.nanoTime();
        writeDebugFile(settingMain, () -> String.format(TASK_NAME + "\t%d\t%d\t%d", factoryId, count[0], end - start));
    }

    // for test
    public static void main(String[] args) {
        try (BenchOnlineShowQuantityTask task = new BenchOnlineShowQuantityTask(0)) {
            try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
                task.setDao(null, manager);

                task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

                task.execute();
            }
        }
    }
}
