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
package com.tsurugidb.benchmark.costaccounting.online;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.NavigableMap;
import java.util.TreeMap;

import org.junit.jupiter.api.Test;

public class BenchOnlineThreadTest {

    @Test
    void exampleGetTaskRandom() {
        NavigableMap<Integer, String> map = new TreeMap<>();
        map.put(20, "A");
        map.put(20 + 30, "B");
        map.put(20 + 30 + 50, "C");

        for (int i = 0; i < 20; i++) {
            int key = i;
            assertEquals("A", map.higherEntry(key).getValue(), "key=" + key);
        }
        for (int i = 0; i < 30; i++) {
            int key = 20 + i;
            assertEquals("B", map.higherEntry(key).getValue(), "key=" + key);
        }
        for (int i = 0; i < 50; i++) {
            int key = 20 + 30 + i;
            assertEquals("C", map.higherEntry(key).getValue(), "key=" + key);
        }
    }
}
