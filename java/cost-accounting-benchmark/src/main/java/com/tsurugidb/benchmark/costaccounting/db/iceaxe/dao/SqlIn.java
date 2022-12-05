package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import com.tsurugidb.iceaxe.statement.TgVariable;

public class SqlIn {

    private final String columnName;
    private final StringBuilder sb = new StringBuilder(32);

    public SqlIn(String columnName) {
        this.columnName = columnName;
    }

    public void add(TgVariable<?> variable) {
        var sqlName = variable.sqlName();
        add(sqlName);
    }

    public void add(String sqlName) {
        if (sb.length() != 0) {
            sb.append(",");
        }
        sb.append(sqlName);
    }

    @Override
    public String toString() {
        return columnName + " in (" + sb + ")";
    }
}
