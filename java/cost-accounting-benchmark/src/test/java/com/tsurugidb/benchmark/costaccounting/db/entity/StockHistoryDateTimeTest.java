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
package com.tsurugidb.benchmark.costaccounting.db.entity;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import org.junit.jupiter.api.Test;

class StockHistoryDateTimeTest {

    @Test
    void testCompareTo() {
        var entity17_00 = create(2024, 1, 17, 0);
        var entity17_12 = create(2024, 1, 17, 12);
        var entity17_23 = create(2024, 1, 17, 23);
        var entity16_00 = create(2024, 1, 16, 0);
        var entity16_12 = create(2024, 1, 16, 12);
        var entity16_23 = create(2024, 1, 16, 23);
        var list = new ArrayList<>(List.of(entity17_00, entity17_23, entity16_23, entity16_12, entity16_00, entity17_12));
        Collections.sort(list);

        var expected = List.of(entity16_00, entity16_12, entity16_23, entity17_00, entity17_12, entity17_23);
        assertEquals(expected, list);
    }

    static StockHistoryDateTime create(int y, int m, int d, int h) {
        var entity = new StockHistoryDateTime();
        entity.setSDate(LocalDate.of(y, m, d));
        entity.setSTime(LocalTime.of(h, 0));
        return entity;
    }
}
