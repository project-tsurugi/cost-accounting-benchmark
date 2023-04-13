package com.tsurugidb.benchmark.costaccounting.init;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.StockHistoryDao;

public class InitialData06StockHistory extends InitialData {

    public static void main(String... args) throws Exception {
        new InitialData06StockHistory().main();
    }

    public InitialData06StockHistory() {
        super(null);
    }

    private void main() {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            deleteStockHistory();
        }

        logEnd();
    }

    private void deleteStockHistory() {
        StockHistoryDao dao = dbManager.getStockHistoryDao();

        var setting = getSetting(StockHistoryDao.TABLE_NAME);
        dbManager.execute(setting, () -> {
            dao.truncate();
        });
    }
}
