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
    public static final String TASK_NAME = "show-weight";

    private TgTmSetting settingMain;
    private double coverRate;

    public BenchOnlineShowWeightTask(int taskId) {
        super(TASK_NAME, taskId);
    }

    @Override
    public void initializeSetting() {
        this.settingMain = config.getSetting(LOG, this, () -> TgTxOption.ofRTX());
        setTxOptionDescription(settingMain);
        this.coverRate = config.getCoverRateForTask(title);
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
        long start = System.nanoTime();
        ItemManufacturingMasterDao itemManufacturingMasterDao = dbManager.getItemManufacturingMasterDao();
        List<Integer> list = itemManufacturingMasterDao.selectIdByFactory(factoryId, date);
        if (list.isEmpty()) {
            return -1;
        }
        long end = System.nanoTime();
        writeDebugFile(settingMain, () -> String.format(TASK_NAME + "1\t%d\t%d\t%d", factoryId, list.size(), end - start));
        var selector = new RandomKeySelector<Integer>(list, random.getRawRandom(), 0, coverRate);
        return selector.get();
    }

    protected void executeMain(int productId) {
        ResultTableDao resultTableDao = dbManager.getResultTableDao();
        long start = System.nanoTime();
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
        long end = System.nanoTime();
        writeDebugFile(settingMain, () -> String.format(TASK_NAME + "2\t%d\t%d\t%d\t%d", factoryId, productId, list.size(), end - start));
    }

    // for test
    public static void main(String[] args) {
        try (BenchOnlineShowWeightTask task = new BenchOnlineShowWeightTask(0)) {
            try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
                task.setDao(null, manager);

                task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

                task.execute();
            }
        }
    }
}
