package com.tsurugidb.benchmark.costaccounting.generate.ddl;

import java.io.BufferedWriter;

import org.apache.poi.ss.usermodel.Row;

import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;

// TODO dataType
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
//          return getTypeWithSize(row, "decimal");
            return "double";
        case "variable text":
            return getTypeWithSize(row, "varchar");
        case "date":
//          return "date";
            return "char(10)"; // yyyy-MM-dd
        default:
            throw new UnsupportedOperationException(typeName);
        }
    }
}
