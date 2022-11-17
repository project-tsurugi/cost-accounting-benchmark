package com.tsurugidb.benchmark.costaccounting.init;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.FactoryMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;

public class InitialData02FactoryMaster extends InitialData {

    public static void main(String... args) throws Exception {
        int factorySize = BenchConst.initFactorySize();
        new InitialData02FactoryMaster().main(factorySize);
    }

    public InitialData02FactoryMaster() {
        super(null);
    }

    private void main(int size) {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            generateFactoryMaster(size);
        }

        logEnd();
    }

    private void generateFactoryMaster(int size) {
        FactoryMasterDao dao = dbManager.getFactoryMasterDao();

        dbManager.execute(TX_INIT, () -> {
            dao.deleteAll();
            insertFactoryMaster(size, dao);
        });
    }

    private void insertFactoryMaster(int size, FactoryMasterDao dao) {
        for (int i = 0; i < size; i++) {
            int fId = i + 1;

            FactoryMaster entity = new FactoryMaster();
            entity.setFId(fId);
            entity.setFName("Factory" + fId);

            dao.insert(entity);
        }
    }
}
