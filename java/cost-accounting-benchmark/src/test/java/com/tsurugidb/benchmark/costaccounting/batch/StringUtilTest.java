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
package com.tsurugidb.benchmark.costaccounting.batch;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;

import org.junit.jupiter.api.Test;

class StringUtilTest {

    @Test
    void testToStringListOfInteger() {
        assertEquals("[]", StringUtil.toString(Arrays.asList()));
        assertEquals("[1]", StringUtil.toString(Arrays.asList(1)));
        assertEquals("[1, 2]", StringUtil.toString(Arrays.asList(1, 2)));
        assertEquals("[1, 3]", StringUtil.toString(Arrays.asList(1, 3)));
        assertEquals("[1-3]", StringUtil.toString(Arrays.asList(1, 2, 3)));
        assertEquals("[10]", StringUtil.toString(Arrays.asList(10)));
        assertEquals("[10, 11]", StringUtil.toString(Arrays.asList(10, 11)));
        assertEquals("[10-12]", StringUtil.toString(Arrays.asList(10, 11, 12)));
        assertEquals("[1-3, 5]", StringUtil.toString(Arrays.asList(1, 2, 3, 5)));
        assertEquals("[1-3, 5, 6]", StringUtil.toString(Arrays.asList(1, 2, 3, 5, 6)));
        assertEquals("[1-3, 5-7]", StringUtil.toString(Arrays.asList(1, 2, 3, 5, 6, 7)));
        assertEquals("[1, 3-5]", StringUtil.toString(Arrays.asList(1, 3, 4, 5)));
        assertEquals("[1-3, 5, 7-9]", StringUtil.toString(Arrays.asList(1, 2, 3, 5, 7, 8, 9)));
    }
}
