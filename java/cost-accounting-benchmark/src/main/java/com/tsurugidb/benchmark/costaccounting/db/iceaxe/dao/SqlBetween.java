package com.tsurugidb.benchmark.costaccounting.db.iceaxe.dao;

import com.tsurugidb.iceaxe.statement.TgVariable;

public class SqlBetween {

    private final String exp1;
    private final String exp2;
    private final String exp3;

    public SqlBetween(TgVariable<?> exp1, String exp2, String exp3) {
        this.exp1 = exp1.sqlName();
        this.exp2 = exp2;
        this.exp3 = exp3;
    }

    @Override
    public String toString() {
        return "(" + exp2 + " <= " + exp1 + " and " + exp1 + " <= " + exp3 + ")";
    }
}
