package com.example.nedo.init;

import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;

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

        dbManager.execute(() -> {
            dao.deleteAll();
        });
    }
}
