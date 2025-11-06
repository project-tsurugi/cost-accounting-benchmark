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
package com.tsurugidb.benchmark.costaccounting.db.domain;

public enum ItemType {
    /** 製品 */
    PRODUCT,
    /** 中間材 */
    WORK_IN_PROCESS,
    /** 原料 */
    RAW_MATERIAL;

    public static ItemType of(String value) {
        return ItemType.valueOf(value.toUpperCase());
    }

    public String getValue() {
        return name().toLowerCase();
    }
}
