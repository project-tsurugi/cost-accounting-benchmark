package com.tsurugidb.benchmark.costaccounting.init;

import java.io.IOException;

import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.DdlGenerator;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class InitialData00CreateTable extends InitialData {

    public static void main(String... args) throws Exception {
        var dbmsType = BenchConst.dbmsType();
        new InitialData00CreateTable().main(dbmsType);
    }

    public InitialData00CreateTable() {
        super(null);
    }

    private void main(DbmsType dbmsType) throws IOException, InterruptedException {
        logStart();

        var generator = DdlGenerator.createDdlGenerator(dbmsType);
        try (var manager = initializeDbManager()) {
            generator.executeDdl(manager);
        } finally {
            shutdown();
        }

        logEnd();
    }
}
