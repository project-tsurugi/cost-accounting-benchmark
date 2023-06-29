package com.tsurugidb.benchmark.costaccounting.init;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;

public class InitialData07ResultTable extends InitialData {

    public static void main(String... args) throws Exception {
        new InitialData07ResultTable().main();
    }

    public InitialData07ResultTable() {
        super(null);
    }

    private void main() {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            deleteResultTable();
        } finally {
            shutdown();
        }

        logEnd();
    }

    private void deleteResultTable() {
        ResultTableDao dao = dbManager.getResultTableDao();

        var setting = getSetting(ResultTableDao.TABLE_NAME);
        dbManager.execute(setting, () -> {
            dao.truncate();
        });
    }
}
