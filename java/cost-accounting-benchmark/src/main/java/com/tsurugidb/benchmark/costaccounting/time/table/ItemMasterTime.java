/*
 * Copyright 2023-2025 Project Tsurugi.
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
import java.text.MessageFormat;
import java.time.LocalDate;
import java.time.temporal.TemporalAdjusters;

import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class ItemMasterTime extends TableTime {

    private ItemMasterDao dao;

    public ItemMasterTime() {
        super(ItemMasterDao.TABLE_NAME);
    }

    @Override
    public void execute() {
        this.dao = dbManager.getItemMasterDao();
        clear();
        insert();
        selectAll();
        selectPoint();
        selectRangeScan();
        deleteAll();
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
            for (int i = 0; i < size; i++) {
                for (int j = sizeAdjustStart; j <= sizeAdjustEnd; j++) {
                    var entity = createItemMaster(i, j);
                    dao.insert(entity);
                }
            }
        });
    }

    private static final LocalDate BASE_DATE = LocalDate.of(2022, 12, 1);

    private ItemMaster createItemMaster(int i, int j) {
        var startDate = BASE_DATE.plusMonths(j);
        var endDate = startDate.with(TemporalAdjusters.lastDayOfMonth());

        var entity = new ItemMaster();
        entity.setIId(i);
        entity.setIEffectiveDate(startDate);
        entity.setIExpiredDate(endDate);
        entity.setIName("name" + i);
        entity.setIType(ItemType.PRODUCT);
        entity.setIUnit("g");
        entity.setIWeightRatio(BigDecimal.valueOf(100));
        entity.setIWeightUnit("g");
        entity.setIPrice(BigDecimal.valueOf(300));
        entity.setIPriceUnit("g");
        return entity;
    }

    private void selectAll() {
        execute("selectAll", 3, () -> {
            var list = dao.selectAll();
            int expected = size * (sizeAdjustEnd - sizeAdjustStart + 1);
            if (list.size() != expected) {
                throw new AssertionError(MessageFormat.format("size unmatch. list={0}, size={1}", list.size(), expected));
            }
        });
    }

    private void selectPoint() {
        execute("select(point)", 3, () -> {
            for (int i = 0; i < size; i++) {
                var entity = dao.selectByKey(i, BASE_DATE);
                if (entity == null) {
                    throw new AssertionError("id=" + i);
                }
            }
        });
    }

    private void selectRangeScan() {
        execute("select(range-scan)", 3, () -> {
            var date = LocalDate.of(2022, 12, 13);
            for (int i = 0; i < size; i++) {
                var entity = dao.selectById(i, date);
                if (entity == null) {
                    throw new AssertionError("id=" + i);
                }
            }
        });
    }

    private void deleteAll() {
        execute("deleteAll", () -> {
            dao.deleteAll();
        });
    }
}
