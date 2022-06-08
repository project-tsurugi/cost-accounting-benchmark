package com.example.nedo.init;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import com.example.nedo.db.CostBenchDbManager;
import com.example.nedo.db.doma2.dao.CostMasterDao;
import com.example.nedo.db.doma2.dao.FactoryMasterDao;
import com.example.nedo.db.doma2.dao.ItemMasterDao;
import com.example.nedo.db.doma2.domain.ItemType;
import com.example.nedo.db.doma2.entity.CostMaster;
import com.example.nedo.db.doma2.entity.ItemMaster;
import com.example.nedo.init.util.DaoSplitTask;

public class InitialData05CostMaster extends InitialData {

    public static void main(String[] args) throws Exception {
        LocalDate batchDate = DEFAULT_BATCH_DATE;
        new InitialData05CostMaster(batchDate).main();
    }

    private final Set<Integer> factoryIdSet = new TreeSet<>();

    public InitialData05CostMaster(LocalDate batchDate) {
        super(batchDate);
    }

    private void main() {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            initializeField();
            generateCostMaster();
        }

        logEnd();
    }

    private void initializeField() {
        {
            FactoryMasterDao dao = dbManager.getFactoryMasterDao();
            dbManager.execute(TX_INIT, () -> {
                List<Integer> list = dao.selectAllId();

                factoryIdSet.clear();
                factoryIdSet.addAll(list);
            });
        }
    }

    private void generateCostMaster() {
        dbManager.execute(TX_INIT, () -> {
            CostMasterDao dao = dbManager.getCostMasterDao();
            dao.deleteAll();
        });

        InitialData03ItemMaster itemMasterData = InitialData03ItemMaster.getDefaultInstance();
        int materialStartId = itemMasterData.getMaterialStartId();
        int materialEndId = itemMasterData.getMaterialEndId();
        executeTask(new CostMasterTask(materialStartId, materialEndId));
        joinAllTask();
    }

    @SuppressWarnings("serial")
    private class CostMasterTask extends DaoSplitTask {
        public CostMasterTask(int startId, int endId) {
            super(dbManager, TX_INIT, startId, endId);
        }

        @Override
        protected DaoSplitTask createTask(int startId, int endId) {
            return new CostMasterTask(startId, endId);
        }

        @Override
        protected void execute(int iId) {
            insertCostMaster(iId, dbManager.getItemMasterDao(), dbManager.getCostMasterDao());
        }
    }

    private static final BigDecimal Q_START = new BigDecimal("1.0");
    private static final BigDecimal Q_END = new BigDecimal("10.0");
    private static final BigDecimal Q_MULT = new BigDecimal("100");
    private static final BigDecimal A_START = new BigDecimal("0.90");
    private static final BigDecimal A_END = new BigDecimal("1.10");
    private static final BigDecimal A_MIN = new BigDecimal("0.01");

    private void insertCostMaster(int materialId, ItemMasterDao itemDao, CostMasterDao dao) {
        ItemMaster materialEntity = itemDao.selectById(materialId, batchDate);
        if (materialEntity.getIType() != ItemType.RAW_MATERIAL) {
            throw new AssertionError(materialEntity);
        }

        int seed = materialEntity.getIId();

        List<Integer> factoryIds = new ArrayList<>(factoryIdSet);
        int size = factoryIdSet.size() / 2; // 50%
        Set<Integer> factorySet = new HashSet<>();
        while (factorySet.size() < size) {
            int factoryId = getRandomAndRemove(seed++, factoryIds);
            factorySet.add(factoryId);
        }

        for (Integer factoryId : factorySet) {
            CostMaster entity = new CostMaster();
            entity.setCFId(factoryId);
            entity.setCIId(materialEntity.getIId());

            switch (MeasurementUtil.toDefaultUnit(materialEntity.getIUnit())) {
            case "g":
                entity.setCStockUnit("kg");
                break;
            case "mL":
                entity.setCStockUnit("L");
                break;
            case "count":
                entity.setCStockUnit("count");
                break;
            default:
                throw new RuntimeException(materialEntity.getIUnit());
            }

            entity.setCStockQuantity(random(seed++, Q_START, Q_END).multiply(Q_MULT));

            BigDecimal price = materialEntity.getIPrice();
            BigDecimal stock = MeasurementUtil.convertUnit(entity.getCStockQuantity(), entity.getCStockUnit(), materialEntity.getIPriceUnit());
            BigDecimal amount = price.multiply(random(seed++, A_START, A_END).multiply(stock));
            if (amount.compareTo(A_MIN) < 0) {
                amount = A_MIN;
            }
            entity.setCStockAmount(amount);

            dao.insert(entity);
        }
    }
}
