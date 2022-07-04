package com.tsurugidb.benchmark.costaccounting.ddl.ddl;

import java.io.BufferedWriter;

import org.apache.poi.ss.usermodel.Row;

import com.tsurugidb.benchmark.costaccounting.ddl.common.TableSheet;

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
        default:
            throw new UnsupportedOperationException(typeName);
        }
    }
}
