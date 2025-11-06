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
package com.tsurugidb.benchmark.costaccounting.init.util;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.junit.jupiter.api.Test;

class AmplificationSizeTest {

    @Test
    void testAmplificationCount() {
        {
            AmplificationSize a = new AmplificationSize(3);
            int count = 0;
            final int SIZE = 10;
            for (int i = 1; i <= SIZE; i++) {
                int actual = a.amplificationSize(i);
                assertEquals(2, actual);
                count += 1 + actual;
            }
            assertEquals(SIZE * 3, count);
        }
        {
            AmplificationSize a = new AmplificationSize(3.0);
            for (int i = 1; i <= 10; i++) {
                assertEquals(2, a.amplificationSize(i));
            }
        }

        {
            AmplificationSize a = new AmplificationSize(1.5);
            int count = 0;
            final int SIZE = 10;
            for (int i = 0; i < SIZE; i++) {
                int actual = a.amplificationSize(i + 1);
                assertEquals((i % 2 == 0) ? 1 : 0, actual);
                count += 1 + actual;
            }
            assertEquals(SIZE * 1.5, count);
        }
        {
            AmplificationSize a = new AmplificationSize(1.25);
            for (int i = 0; i < 10; i++) {
                assertEquals((i % 4 == 0) ? 1 : 0, a.amplificationSize(i + 1));
            }
        }
        {
            AmplificationSize a = new AmplificationSize(1.6);
            for (int i = 0; i < 10; i++) {
                assertEquals((i % 5 < 3) ? 1 : 0, a.amplificationSize(i + 1));
            }
        }
    }
}
