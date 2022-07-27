package com.tsurugidb.benchmark.costaccounting.init;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.DbmsType;
import com.tsurugidb.benchmark.costaccounting.db.doma2.CostBenchDbManagerDoma2;
import com.tsurugidb.benchmark.costaccounting.db.raw.CostBenchDbManagerJdbc2;
import com.tsurugidb.benchmark.costaccounting.generate.ddl.DdlGenarator;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class InitialData00CreateTable extends InitialData {
    private static final Logger LOG = LoggerFactory.getLogger(InitialData00CreateTable.class);

    public static void main(String[] args) throws Exception {
        var dbmsType = BenchConst.dbmsType();
        new InitialData00CreateTable().main(dbmsType);
    }

    public InitialData00CreateTable() {
        super(null);
    }

    private void main(DbmsType dbmsType) throws IOException {
        logStart();

        var generator = DdlGenarator.createDdlGenerator(dbmsType);
        try (var manager = getDbManager()) {
            generator.executeDdl(manager);
        }

        logEnd();
    }

    private CostBenchDbManager getDbManager() {
        var manager = initializeDbManager();
        if (manager instanceof CostBenchDbManagerDoma2) {
            manager.close();
            manager = new CostBenchDbManagerJdbc2();
            LOG.info("changed {}", manager.getClass().getSimpleName());
        }
        return manager;
    }
}
