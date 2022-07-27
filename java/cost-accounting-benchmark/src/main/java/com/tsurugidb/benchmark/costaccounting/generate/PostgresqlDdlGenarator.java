package com.tsurugidb.benchmark.costaccounting.generate;

import java.io.BufferedWriter;

import com.tsurugidb.benchmark.costaccounting.generate.ddl.DdlGenarator;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.PostgresqlTableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.TableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;

public class PostgresqlDdlGenarator extends DdlGenarator {

    public static void main(String[] args) throws Exception {
        new PostgresqlDdlGenarator().writeDdlFile("ddl-postgresql.txt");
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new PostgresqlTableDdlWriter(table, writer);
    }
}
