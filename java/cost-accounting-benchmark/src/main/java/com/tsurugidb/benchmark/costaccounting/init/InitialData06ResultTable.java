package com.tsurugidb.benchmark.costaccounting.init;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.doma2.dao.ResultTableDao;

public class InitialData06ResultTable extends InitialData {

    public static void main(String[] args) throws Exception {
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

        dbManager.execute(TX_INIT, () -> {
            dao.deleteAll();
        });
    }
}
