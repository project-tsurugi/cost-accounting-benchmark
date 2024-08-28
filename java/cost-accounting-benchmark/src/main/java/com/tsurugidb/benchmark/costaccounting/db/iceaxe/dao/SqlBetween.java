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
package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

public class SqlBetween {

    private final String exp1;
    private final String exp2;
    private final String exp3;

    public SqlBetween(TgBindVariable<?> exp1, String exp2, String exp3) {
        this.exp1 = exp1.sqlName();
        this.exp2 = exp2;
        this.exp3 = exp3;
    }

    @Override
    public String toString() {
        if (BenchConst.WORKAROUND) {
            return "(" + exp2 + " <= " + exp1 + " and " + exp1 + " <= " + exp3 + ")";
        }
        return exp1 + " between " + exp2 + " and " + exp3;
    }
}
