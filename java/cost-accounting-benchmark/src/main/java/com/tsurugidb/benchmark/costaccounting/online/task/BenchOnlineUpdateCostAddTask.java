package com.tsurugidb.benchmark.costaccounting.online.task;

import java.math.BigDecimal;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtil;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 原価の変更（在庫増加）
 */
public class BenchOnlineUpdateCostAddTask extends BenchOnlineTask {
    public static final String TASK_NAME = "update-cost-add";

    private final TgTmSetting settingPre;
    private final TgTmSetting settingMain;

    public BenchOnlineUpdateCostAddTask() {
        super(TASK_NAME);
        this.settingPre = TgTmSetting.ofAlways(TgTxOption.ofRTX().label(TASK_NAME + ".pre"));
        this.settingMain = getSetting(() -> TgTxOption.ofLTX(CostMasterDao.TABLE_NAME));
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
            executeIncrease(itemId);
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
    private static final BigDecimal A_START = new BigDecimal("0.90");
    private static final BigDecimal A_END = new BigDecimal("1.10");

    protected void executeIncrease(int itemId) {
        logTarget("increase product=%d", itemId);

        // 増やす在庫数
        BigDecimal quantity = random.random(Q_START, Q_END).multiply(Q_MULTI);

        // 在庫単位取得
        CostMasterDao costMasterDao = dbManager.getCostMasterDao();
        CostMaster cost = costMasterDao.selectById(factoryId, itemId, false);

        // 増やす金額
        BigDecimal amount;
        {
            ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
            ItemMaster item = itemMasterDao.selectById(itemId, date);
            BigDecimal price = MeasurementUtil.convertPriceUnit(item.getIPrice(), item.getIPriceUnit(), cost.getCStockUnit());
            amount = price.multiply(random.random(A_START, A_END)).multiply(quantity);
        }

        // 更新
        int r = costMasterDao.updateIncrease(cost, quantity, amount);
        assert r == 1;
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineUpdateCostAddTask task = new BenchOnlineUpdateCostAddTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
