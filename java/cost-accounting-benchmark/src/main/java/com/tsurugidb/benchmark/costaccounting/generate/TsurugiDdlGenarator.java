package com.tsurugidb.benchmark.costaccounting.generate;

import java.io.BufferedWriter;

import com.tsurugidb.benchmark.costaccounting.generate.ddl.DdlGenarator;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.TableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.TsurugiTableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.generate.util.TableSheet;

public class TsurugiDdlGenarator extends DdlGenarator {

    public static void main(String[] args) throws Exception {
        new TsurugiDdlGenarator().writeDdlFile("ddl-tsurugi.txt");
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new TsurugiTableDdlWriter(table, writer);
    }
}
