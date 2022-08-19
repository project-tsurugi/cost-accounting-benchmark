package com.tsurugidb.benchmark.costaccounting.generate;

import java.io.BufferedWriter;

import com.tsurugidb.benchmark.costaccounting.generate.ddl.DdlGenerator;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.TableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.TsurugiTableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;

public class TsurugiDdlGenerator extends DdlGenerator {

    public static void main(String[] args) throws Exception {
        new TsurugiDdlGenerator().writeDdlFile("ddl-tsurugi.txt");
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new TsurugiTableDdlWriter(table, writer);
    }
}
