package com.tsurugidb.benchmark.costaccounting.ddl;

import java.io.BufferedWriter;

import com.tsurugidb.benchmark.costaccounting.ddl.common.TableSheet;
import com.tsurugidb.benchmark.costaccounting.ddl.ddl.DdlGenarator;
import com.tsurugidb.benchmark.costaccounting.ddl.ddl.TableDdlWriter;
import com.tsurugidb.benchmark.costaccounting.ddl.ddl.TsurugiTableDdlWriter;

public class TsurugiDdlGenarator extends DdlGenarator {

    public static void main(String[] args) throws Exception {
        new TsurugiDdlGenarator().execute();
    }

    @Override
    protected String getDdlFileName() {
        return "ddl-tsurugi.txt";
    }

    @Override
    protected TableDdlWriter createTableDdlWriter(TableSheet table, BufferedWriter writer) {
        return new TsurugiTableDdlWriter(table, writer);
    }
}
