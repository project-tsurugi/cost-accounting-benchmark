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
package com.tsurugidb.benchmark.costaccounting.generate.ddl;

import java.io.BufferedWriter;

import org.apache.poi.ss.usermodel.Row;

import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;

public class TsurugiTableDdlWriter extends TableDdlWriter {

    public TsurugiTableDdlWriter(TableSheet table, BufferedWriter writer) {
        super(table, writer);
    }

    @Override
    protected String getType(Row row, String typeName) {
        switch (typeName) {
        case "unique ID":
            return "int";
        case "unsigned numeric":
            return getTypeWithSize(row, "decimal");
        case "variable text":
            return getTypeWithSize(row, "varchar");
        case "date":
            return "date";
        case "time":
            return "time";
        default:
            throw new UnsupportedOperationException(typeName);
        }
    }
}
