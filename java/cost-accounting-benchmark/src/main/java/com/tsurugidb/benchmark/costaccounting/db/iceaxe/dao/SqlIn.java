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
package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import com.tsurugidb.iceaxe.sql.parameter.TgBindVariable;

public class SqlIn {

    private final String columnName;
    private final StringBuilder sb = new StringBuilder(32);

    public SqlIn(String columnName) {
        this.columnName = columnName;
    }

    public void add(TgBindVariable<?> variable) {
        var sqlName = variable.sqlName();
        add(sqlName);
    }

    public void add(String sqlName) {
//      if (BenchConst.WORKAROUND) {
//          addWorkaround(sqlName);
//          return;
//      }

        if (sb.length() != 0) {
            sb.append(",");
        }
        sb.append(sqlName);
    }

    @SuppressWarnings("unused")
    private void addWorkaround(String sqlName) {
        if (sb.length() != 0) {
            sb.append(" or ");
        }
        sb.append(columnName);
        sb.append(" = ");
        sb.append(sqlName);
    }

    @Override
    public String toString() {
//      if (BenchConst.WORKAROUND) {
//          return "(" + sb + ")";
//      }

        return columnName + " in (" + sb + ")";
    }
}
