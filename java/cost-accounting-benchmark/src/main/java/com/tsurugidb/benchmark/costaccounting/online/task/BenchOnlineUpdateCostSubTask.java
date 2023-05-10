package com.tsurugidb.benchmark.costaccounting.online.task;

import java.math.BigDecimal;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.online.OnlineConfig;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 原価の変更（在庫減少）
 */
public class BenchOnlineUpdateCostSubTask extends BenchOnlineTask {
    public static final String TASK_NAME = "update-cost-sub";

    private TgTmSetting settingPre;
    private TgTmSetting settingMain;

    public BenchOnlineUpdateCostSubTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting(OnlineConfig config) {
        this.settingPre = TgTmSetting.ofAlways(TgTxOption.ofRTX().label(TASK_NAME + ".pre"));
        this.settingMain = config.getSetting(LOG, this, () -> TgTxOption.ofLTX(CostMasterDao.TABLE_NAME));
        setTxOptionDescription(settingMain);
    }

    @Override
    protected boolean execute1() {
        Integer itemId = dbManager.execute(settingPre, () -> {
            return selectRandomItem();
        });
        if (itemId == null) {
            return false;
        }

        dbManager.execute(settingMain, () -> {
            executeDecrease(itemId);
        });
        return true;
    }

    private Integer selectRandomItem() {
        CostMasterDao costMasterDao = dbManager.getCostMasterDao();
        List<Integer> list = costMasterDao.selectIdByFactory(factoryId);
        if (list.isEmpty()) {
            return null;
        }
        int i = random.nextInt(list.size());
        return list.get(i);
    }

    private static final BigDecimal Q_START = new BigDecimal("1.0");
    private static final BigDecimal Q_END = new BigDecimal("10.0");
    private static final BigDecimal Q_MULTI = new BigDecimal("100");

    protected void executeDecrease(int itemId) {
        logTarget("decrease product=%d", itemId);

        // 減らす在庫数
        BigDecimal quantity = random.random(Q_START, Q_END).multiply(Q_MULTI);

        // 在庫数量取得
        CostMasterDao costMasterDao = dbManager.getCostMasterDao();
        CostMaster cost = costMasterDao.selectById(factoryId, itemId, true);

        // 更新
        BigDecimal after = cost.getCStockQuantity().subtract(quantity);
        if (after.compareTo(BigDecimal.ZERO) <= 0) {
            int r = costMasterDao.updateZero(cost);
            assert r == 1;
        } else {
            int r = costMasterDao.updateDecrease(cost, quantity);
            assert r == 1;
        }
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineUpdateCostSubTask task = new BenchOnlineUpdateCostSubTask(0);

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
