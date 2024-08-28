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
package com.tsurugidb.benchmark.costaccounting.init;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.init.ConstraintCheck.ConstraintCheckException;

class ConstraintCheckTest {

    @Test
    void testCheck() {
        String tableName = "test";

        List<ItemMaster> allList = new ArrayList<>();
        allList.add(create(1, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(2, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(2, LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 31)));
        allList.add(create(3, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(3, LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 31)));
        allList.add(create(3, LocalDate.of(2020, 11, 1), LocalDate.of(2020, 11, 30)));

        ConstraintCheck target = new ConstraintCheck();
        assertDoesNotThrow(() -> target.checkDateRange(tableName, allList, target::getKey));
    }

    @Test
    void testCheck_error() {
        String tableName = "test";

        List<ItemMaster> allList = new ArrayList<>();
        allList.add(create(1, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(2, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(2, LocalDate.of(2020, 10, 1), LocalDate.of(2020, 10, 31)));
        allList.add(create(3, LocalDate.of(2020, 9, 1), LocalDate.of(2020, 9, 30)));
        allList.add(create(3, LocalDate.of(2020, 9, 30), LocalDate.of(2020, 10, 31)));
        allList.add(create(3, LocalDate.of(2020, 11, 1), LocalDate.of(2020, 11, 30)));

        ConstraintCheck target = new ConstraintCheck();
        ConstraintCheckException e = assertThrows(ConstraintCheckException.class, () -> target.checkDateRange(tableName, allList, target::getKey));
        assertEquals("constraint check error test[3]", e.getMessage());
    }

    private ItemMaster create(int id, LocalDate startDate, LocalDate endDate) {
        ItemMaster entity = new ItemMaster();
        entity.setIId(id);
        entity.setIEffectiveDate(startDate);
        entity.setIExpiredDate(endDate);
        return entity;
    }
}
