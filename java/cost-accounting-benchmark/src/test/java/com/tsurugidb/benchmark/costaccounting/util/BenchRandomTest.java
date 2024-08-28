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
package com.tsurugidb.benchmark.costaccounting.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;

import org.junit.jupiter.api.Test;

class BenchRandomTest {

    private final BenchRandom random = new BenchRandom();

    @Test
    void testRandomIntInt() {
        for (int i = 0; i < 10000; i++) {
            int r = random.random(1, 10);
            assertTrue(1 <= r && r <= 10, () -> "r=" + r);
        }
    }

    private static final BigDecimal D_START = new BigDecimal("1.0");
    private static final BigDecimal D_END = new BigDecimal("10.0");

    @Test
    void testRandomBigDecimalBigDecimal() {
        for (int i = 0; i < 10000; i++) {
            BigDecimal r = random.random(D_START, D_END);
            assertTrue(D_START.compareTo(r) <= 0 && r.compareTo(D_END) <= 0, () -> "r=" + r);
            assertEquals(1, r.scale());
        }
    }
}
