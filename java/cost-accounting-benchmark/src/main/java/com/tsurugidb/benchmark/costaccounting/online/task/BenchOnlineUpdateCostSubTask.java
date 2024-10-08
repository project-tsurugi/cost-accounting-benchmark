/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.online.task;

import java.math.BigDecimal;
import java.util.List;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

/**
 * 原価の変更（在庫減少）
 */
public class BenchOnlineUpdateCostSubTask extends BenchOnlineTask {
    public static final String TASK_NAME = "update-cost-sub";

    private TgTmSetting settingPre;
    private TgTmSetting settingMain;
    private double coverRate;

    public BenchOnlineUpdateCostSubTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting() {
        this.settingPre = TgTmSetting.ofAlways(TgTxOption.ofRTX().label(TASK_NAME + ".pre"));
        this.settingMain = config.getSetting(LOG, this, () -> {
            if (BenchConst.useReadArea()) {
                return TgTxOption.ofLTX(BenchConst.DEFAULT_TX_OPTION).addWritePreserve(CostMasterDao.TABLE_NAME) //
                        .addInclusiveReadArea(CostMasterDao.TABLE_NAME);
            } else {
                return TgTxOption.ofLTX(CostMasterDao.TABLE_NAME);
            }
        });
        setTxOptionDescription(settingMain);
        this.coverRate = config.getCoverRateForTask(title);
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
        var selector = new RandomKeySelector<Integer>(list, random.getRawRandom(), 0, coverRate);
        return selector.get();
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
        try (BenchOnlineUpdateCostSubTask task = new BenchOnlineUpdateCostSubTask(0)) {
            try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
                task.setDao(null, manager);

                task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

                task.execute();
            }
        }
    }
}
