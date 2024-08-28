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
package com.tsurugidb.benchmark.costaccounting.generate.ddl;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.UncheckedIOException;
import java.util.List;

import org.apache.poi.ss.usermodel.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;
import com.tsurugidb.benchmark.costaccounting.generate.util.WriterWrapper;
import com.tsurugidb.iceaxe.util.function.IoRunnable;

public abstract class TableDdlWriter extends WriterWrapper {
    private static final Logger LOG = LoggerFactory.getLogger(TableDdlWriter.class);

    private final TableSheet table;

    public TableDdlWriter(TableSheet table, BufferedWriter writer) {
        super(writer, "  ");
        this.table = table;
    }

    public void write() throws IOException {
        LOG.info("sheet={}", table.getSheetName());

        writeComment();
        writeDrop(";");
        writeCreate(";");
    }

    public String getDropDdl() throws IOException, InterruptedException {
        return getDdl(() -> writeDrop(""));
    }

    public String getCreateDdl() throws IOException, InterruptedException {
        return getDdl(() -> writeCreate(""));
    }

    private String getDdl(IoRunnable runnable) throws IOException, InterruptedException {
        try (var writer = new StringWriter(1024)) {
            setWriter(writer);
            runnable.run();
            return writer.toString();
        }
    }

    // write ddl

    protected void writeComment() throws IOException {
        writeln();

        String tableLogicalName = table.getTableLogicalName();
        writeln("-- ", tableLogicalName);
    }

    protected void writeDrop(String eos) throws IOException {
        String tableName = table.getTableName();
        LOG.info("table={}", tableName);
        writeln("drop table ", tableName, eos);
    }

    protected void writeCreate(String eos) throws IOException {
        String tableName = table.getTableName();
        writeln("create table ", tableName);

        writeln("(");
        table.getRows().forEachOrdered(row -> {
            try {
                writeField(row);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        });

        List<String> pkList = table.getPrimaryKeyList();
        if (!pkList.isEmpty()) {
            String keys = String.join(", ", pkList);
            writeln(1, ",primary key(", keys, ")");
        }

        writeln(")", eos);
    }

    protected void writeField(Row row) throws IOException {
        String name = table.getColumnName(row);
        if (name == null) {
            return;
        }

        String desc = table.getColumnLogicalName(row);
        if (desc == null) {
            desc = "";
        } else {
            desc = " -- " + desc;
        }

        String type = getType(row);

        String comma;
        if (table.hasNext(row)) {
            comma = ",";
        } else {
            comma = "";
        }

        writeln(1, name, " ", type, comma, desc);
    }

    protected String getType(Row row) {
        String typeName = table.getColumnType(row);
        if (typeName == null) {
            return "";
        }
        return getType(row, typeName);
    }

    protected abstract String getType(Row row, String typeName);

    protected String getTypeWithSize(Row row, String type) {
        Integer size = table.getColumnTypeSize(row);
        if (size == null) {
            return type;
        } else {
            Integer scale = table.getColumnTypeScale(row);
            if (scale == null) {
                return type + "(" + size + ")";
            } else {
                return type + "(" + size + ", " + scale + ")";
            }
        }
    }
}
