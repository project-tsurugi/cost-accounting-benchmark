package com.example.nedo.online.task;

import java.math.BigDecimal;
import java.util.List;

import com.example.nedo.init.InitialData;
import com.example.nedo.init.MeasurementUtil;
import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.jdbc.doma2.dao.CostMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.entity.CostMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

/**
 * 原価の変更
 */
public class BenchOnlineUpdateCostTask extends BenchOnlineTask {

    public BenchOnlineUpdateCostTask() {
        super("update-cost");
    }

    @Override
    protected boolean execute1() {
        return dbManager.execute(() -> {
            CostMaster cost = selectRandomItem();
            if (cost == null) {
                return false;
            }

            int pattern = random.random(0, 1);
            if (pattern == 0) {
                executeIncrease(cost);
            } else {
                executeDecrease(cost);
            }

            return true;
        });
    }

    private CostMaster selectRandomItem() {
        CostMasterDao costMasterDao = dbManager.getCostMasterDao();
        List<CostMaster> list = costMasterDao.selectByFactory(factoryId);
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

    protected void executeIncrease(CostMaster cost) {
        logTarget("increase product=%d", cost.getCIId());

        // 増やす在庫数
        BigDecimal quantity = random.random(Q_START, Q_END).multiply(Q_MULTI);

        // 増やす金額
        BigDecimal amount;
        {
            ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
            ItemMaster item = itemMasterDao.selectById(cost.getCIId(), date);
            BigDecimal price = MeasurementUtil.convertPriceUnit(item.getIPrice(), item.getIPriceUnit(), cost.getCStockUnit());
            amount = price.multiply(random.random(A_START, A_END)).multiply(quantity);
        }

        // 更新
        CostMasterDao costMasterDao = dbManager.getCostMasterDao();
        int r = costMasterDao.updateIncrease(cost, quantity, amount);
        assert r == 1;
    }

    protected void executeDecrease(CostMaster cost) {
        logTarget("decrease product=%d", cost.getCIId());

        CostMasterDao costMasterDao = dbManager.getCostMasterDao();
        cost = costMasterDao.lock(cost);

        // 減らす在庫数
        BigDecimal quantity = random.random(Q_START, Q_END).multiply(Q_MULTI);

        BigDecimal after = cost.getCStockQuantity().subtract(quantity);
        if (after.compareTo(BigDecimal.ZERO) <= 0) {
            executeIncrease(cost);
            return;
        }

        // 更新
        int r = costMasterDao.updateDecrease(cost, quantity);
        assert r == 1;
    }

    // for test
    public static void main(String[] args) {
        BenchOnlineUpdateCostTask task = new BenchOnlineUpdateCostTask();

        try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
            task.setDao(manager);

            task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

            task.execute();
        }
    }
}
