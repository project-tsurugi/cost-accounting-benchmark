package com.tsurugidb.benchmark.costaccounting.generate;

import java.io.BufferedWriter;

import com.tsurugidb.benchmark.costaccounting.generate.ddl.DdlGenarator;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.OracleTableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.TableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;

public class OracleDdlGenarator extends DdlGenarator {

    public static void main(String[] args) throws Exception {
        new OracleDdlGenarator().writeDdlFile("ddl-oracle.txt");
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new OracleTableDdlWriter(table, writer);
    }
}
