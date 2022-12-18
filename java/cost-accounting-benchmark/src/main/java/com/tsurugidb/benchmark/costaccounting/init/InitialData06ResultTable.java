package com.tsurugidb.benchmark.costaccounting.init;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;

public class InitialData06ResultTable extends InitialData {

    public static void main(String... args) throws Exception {
        new InitialData06ResultTable().main();
    }

    public InitialData06ResultTable() {
        super(null);
    }

    private void main() {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            deleteResultTable();
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
