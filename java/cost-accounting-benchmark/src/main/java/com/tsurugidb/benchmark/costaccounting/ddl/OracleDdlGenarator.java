package com.tsurugidb.benchmark.costaccounting.ddl;

import java.io.BufferedWriter;

import com.tsurugidb.benchmark.costaccounting.ddl.common.TableSheet;
import com.tsurugidb.benchmark.costaccounting.ddl.ddl.DdlGenarator;
import com.tsurugidb.benchmark.costaccounting.ddl.ddl.OracleTableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.ddl.ddl.TableDdlWriter;

public class OracleDdlGenarator extends DdlGenarator {

    public static void main(String[] args) throws Exception {
        new OracleDdlGenarator().execute();
    }

    @Override
    protected String getDdlFileName() {
        return "ddl-oracle.txt";
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new OracleTableDdlWriter(table, writer);
    }
}
