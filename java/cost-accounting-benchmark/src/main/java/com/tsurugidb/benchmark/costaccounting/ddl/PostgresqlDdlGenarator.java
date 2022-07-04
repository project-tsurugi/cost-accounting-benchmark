package com.tsurugidb.benchmark.costaccounting.ddl;

import java.io.BufferedWriter;

import com.tsurugidb.benchmark.costaccounting.ddl.common.TableSheet;
import com.tsurugidb.benchmark.costaccounting.ddl.ddl.DdlGenarator;
import com.tsurugidb.benchmark.costaccounting.ddl.ddl.PostgresqlTableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.ddl.ddl.TableDdlWriter;

public class PostgresqlDdlGenarator extends DdlGenarator {

    public static void main(String[] args) throws Exception {
        new PostgresqlDdlGenarator().execute();
    }

    @Override
    protected String getDdlFileName() {
        return "ddl-postgresql.txt";
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new PostgresqlTableDdlWriter(table, writer);
    }
}
