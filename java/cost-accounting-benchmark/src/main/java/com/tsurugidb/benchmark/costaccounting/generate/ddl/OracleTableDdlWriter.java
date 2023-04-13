package com.tsurugidb.benchmark.costaccounting.generate.ddl;

import java.io.BufferedWriter;

import org.apache.poi.ss.usermodel.Row;

import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;

public class OracleTableDdlWriter extends TableDdlWriter {

    public OracleTableDdlWriter(TableSheet table, BufferedWriter writer) {
        super(table, writer);
    }

    @Override
    protected String getType(Row row, String typeName) {
        switch (typeName) {
        case "unique ID":
        case "unsigned numeric":
            return getTypeWithSize(row, "number");
        case "variable text":
            return getTypeWithSize(row, "varchar2");
        case "date":
            return "date";
        case "time":
            return "time";
        default:
            throw new UnsupportedOperationException(typeName);
        }
    }
}
