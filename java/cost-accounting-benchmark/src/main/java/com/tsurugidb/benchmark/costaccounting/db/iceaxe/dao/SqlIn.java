package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import com.tsurugidb.iceaxe.statement.TgVariable;

public class SqlIn {

    private final String columnName;
    private final StringBuilder sb = new StringBuilder(32);

    public SqlIn(String columnName) {
        this.columnName = columnName;
    }

    public void add(TgVariable<?> variable) {
        var name = variable.sqlName();

        if (sb.length() != 0) {
            sb.append(",");
        }
        sb.append(name);
    }

    @Override
    public String toString() {
        return columnName + " in (" + sb + ")";
    }
}
