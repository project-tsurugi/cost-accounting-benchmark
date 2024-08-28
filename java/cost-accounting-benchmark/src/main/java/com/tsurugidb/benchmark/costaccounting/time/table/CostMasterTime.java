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
package com.tsurugidb.benchmark.costaccounting.time.table;

import java.math.BigDecimal;

import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class CostMasterTime extends TableTime {

    private CostMasterDao dao;

    public CostMasterTime() {
        super(CostMasterDao.TABLE_NAME);
    }

    @Override
    public void execute() {
        this.dao = dbManager.getCostMasterDao();
        clear();
        insert();
        selectRangeScan();
    }

    private void clear() {
        if (!BenchConst.timeCommandExecute(tableName, "clear")) {
            return;
        }

        var setting = TgTmSetting.of(TgTxOption.ofLTX(tableName));
        dbManager.execute(setting, () -> {
            dao.truncate();
        });
    }

    private void insert() {
        execute("insert", () -> {
            for (int f = sizeAdjustStart; f <= sizeAdjustEnd; f++) {
                for (int i = 0; i < size; i++) {
                    var entity = createCostMaster(f, i);
                    dao.insert(entity);
                }
            }
        });
    }

    private CostMaster createCostMaster(int factoryId, int i) {
        var entity = new CostMaster();
        entity.setCFId(factoryId);
        entity.setCIId(i);
        entity.setCStockUnit("g");
        entity.setCStockQuantity(BigDecimal.valueOf(100));
        entity.setCStockAmount(BigDecimal.valueOf(300));
        return entity;
    }

    private void selectRangeScan() {
        int fId = (sizeAdjustStart + sizeAdjustEnd) / 2;
        execute("select(range-scan)", 3, () -> {
            dao.selectIdByFactory(fId);
        });
    }
}
