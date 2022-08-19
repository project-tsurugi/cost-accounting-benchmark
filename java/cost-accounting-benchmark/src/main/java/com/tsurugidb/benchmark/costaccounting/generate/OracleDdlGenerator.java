package com.tsurugidb.benchmark.costaccounting.generate;

import java.io.BufferedWriter;

import com.tsurugidb.benchmark.costaccounting.generate.ddl.DdlGenerator;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.OracleTableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.TableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;

public class OracleDdlGenerator extends DdlGenerator {

    public static void main(String[] args) throws Exception {
        new OracleDdlGenerator().writeDdlFile("ddl-oracle.txt");
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new OracleTableDdlWriter(table, writer);
    }
}
