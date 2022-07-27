package com.tsurugidb.benchmark.costaccounting.generate.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;

public class TableSheet extends SheetWrapper {

    public TableSheet(Sheet sheet) {
        super(sheet);
    }

    @Override
    protected int getStartRowIndex() {
        return 2;
    }

    public String getTableLogicalName() {
        return getCellAsString(sheet, "A1");
    }

    public String getTableName() {
        return getCellAsString(sheet, "B1");
    }

    public String getClassName() {
        String tableName = getTableName();
        return toCamelCase(tableName, true);
    }

    public String getColumnLogicalName(Row row) {
        return getCellAsString(row, 0);
    }

    public String getColumnName(Row row) {
        return getCellAsString(row, 1);
    }

    public String getColumnFieldName(Row row) {
        String columnName = getColumnName(row);
        return toCamelCase(columnName, false);
    }

    public String getColumnMethodName(Row row) {
        String columnName = getColumnName(row);
        return toCamelCase(columnName, true);
    }

    public String getColumnType(Row row) {
        return getCellAsString(row, 2);
    }

    public Integer getColumnTypeSize(Row row) {
        return getCellAsInteger(row, 3);
    }

    public Integer getColumnTypeScale(Row row) {
        return getCellAsInteger(row, 4);
    }

    public String getColumnKey(Row row) {
        return getCellAsString(row, 5);
    }

    @Override
    public boolean hasData(Row row) {
        if (row == null) {
            return false;
        }

        String name = getColumnName(row);
        if (name == null) {
            return false;
        }

        return true;
    }

    public List<String> getPrimaryKeyList() {
        List<String> list = new ArrayList<>();
        for (Row row : sheet) {
            String key = getCellAsString(row, 5);
            if ("PK".equals(key)) {
                String name = getCellAsString(row, 1);
                list.add(name);
            }
        }
        return list;
    }

    private Boolean hasDateRange;

    public boolean hasDateRange() {
        if (hasDateRange == null) {
            this.hasDateRange = false;
            for (int i = getStartRowIndex();; i++) {
                Row row = sheet.getRow(i);
                if (!hasData(row)) {
                    break;
                }
                String name = getColumnName(row);
                if (name.endsWith("_effective_date") || name.endsWith("_expired_date")) {
                    this.hasDateRange = true;
                    break;
                }
            }
        }
        return hasDateRange;
    }
}
