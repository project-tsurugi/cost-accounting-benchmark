package com.tsurugidb.benchmark.costaccounting.init;

import java.util.concurrent.atomic.AtomicInteger;

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
            insertFactoryMaster(size);
        } finally {
            shutdown();
        }

        logEnd();
    }

    private void insertFactoryMaster(int size) {
        FactoryMasterDao dao = dbManager.getFactoryMasterDao();

        var setting = getSetting(FactoryMasterDao.TABLE_NAME);
        var insertCount = new AtomicInteger();
        dbManager.execute(setting, () -> {
            dao.truncate();
            insertCount.set(0);
            insertFactoryMaster(size, dao, insertCount);
        });
        LOG.info("insert {}={}", FactoryMasterDao.TABLE_NAME, insertCount.get());
    }

    private void insertFactoryMaster(int size, FactoryMasterDao dao, AtomicInteger insertCount) {
        for (int i = 0; i < size; i++) {
            int fId = i + 1;

            FactoryMaster entity = new FactoryMaster();
            entity.setFId(fId);
            entity.setFName("Factory" + fId);

            dao.insert(entity);
            insertCount.incrementAndGet();
        }
    }
}
