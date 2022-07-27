package com.tsurugidb.benchmark.costaccounting.generate.ddl;

import java.io.BufferedWriter;

import org.apache.poi.ss.usermodel.Row;

import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;

public class PostgresqlTableDdlWriter extends TableDdlWriter {

    public PostgresqlTableDdlWriter(TableSheet table, BufferedWriter writer) {
        super(table, writer);
    }

    @Override
    protected String getType(Row row, String typeName) {
        switch (typeName) {
        case "unique ID":
        case "unsigned numeric":
            return getTypeWithSize(row, "numeric");
        case "variable text":
            return getTypeWithSize(row, "varchar");
        case "date":
            return "date";
        default:
            throw new UnsupportedOperationException(typeName);
        }
    }
}
