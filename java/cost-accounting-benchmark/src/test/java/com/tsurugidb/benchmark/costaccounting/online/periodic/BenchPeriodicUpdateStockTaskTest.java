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
package com.tsurugidb.benchmark.costaccounting.online.periodic;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.costaccounting.db.entity.StockHistoryDateTime;

class BenchPeriodicUpdateStockTaskTest {

    @Test
    void testGetDeleteDateTime() {
        try (var task = new BenchPeriodicUpdateStockTask(1, 1)) {
            for (int listSize = 0; listSize <= 4; listSize++) {
                var list = createList(listSize);

                var actual = task.getDeleteDateTime(list);

                if (listSize == 0) {
                    assertNull(actual);
                } else {
                    assertEquals(create(2024, 1, 17), actual);
                }
            }
        }

        int keepSize = 3;
        try (var task = new BenchPeriodicUpdateStockTask(1, keepSize)) {
            for (int listSize = 0; listSize <= 10; listSize++) {
                var list = createList(listSize);

                var actual = task.getDeleteDateTime(list);

                if (listSize < keepSize) {
                    assertNull(actual);
                } else {
                    var sub = list.subList(0, listSize - keepSize + 1);
                    var last = sub.get(sub.size() - 1);
                    assertEquals(last, actual);
                }
            }
        }
    }

    private static List<StockHistoryDateTime> createList(int size) {
        var list = new ArrayList<StockHistoryDateTime>();
        for (int i = 0; i < size; i++) {
            list.add(create(2024, 1, 17 - size + i + 1));
        }
        return list;
    }

    private static StockHistoryDateTime create(int y, int m, int d) {
        var entity = new StockHistoryDateTime();
        entity.setSDate(LocalDate.of(y, m, d));
        entity.setSTime(LocalTime.of(0, 0));
        return entity;
    }
}
