/*
 * Copyright 2023-2024 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.tsurugidb.benchmark.costaccounting.init;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.init.util.DaoSplitTask;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtil;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class InitialData05CostMaster extends InitialData {

    public static void main(String... args) throws Exception {
        LocalDate batchDate = DEFAULT_BATCH_DATE;
        new InitialData05CostMaster(batchDate).main();
    }

    private final Set<Integer> factoryIdSet = new TreeSet<>();
    private final Map<Integer, ItemMaster> materialMap = BenchConst.init05MaterialCache() ? new HashMap<>(BenchConst.initItemMaterialSize()) : null;

    public InitialData05CostMaster(LocalDate batchDate) {
        super(batchDate);
    }

    private void main() {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            initializeField();
            initMaterialMap();
            insertCostMaster();
        } finally {
            shutdown();
        }

        dumpExplainCounter(dbManager.getItemMasterDao());

        logEnd();
    }

    private void initializeField() {
        {
            FactoryMasterDao dao = dbManager.getFactoryMasterDao();
            var setting = getSetting(() -> TgTxOption.ofRTX());
            dbManager.execute(setting, () -> {
                List<Integer> list = dao.selectAllId();

                factoryIdSet.clear();
                factoryIdSet.addAll(list);
            });
        }
    }

    private void initMaterialMap() {
        if (this.materialMap == null) {
            return;
        }

        LOG.info("select start");
        long start = System.currentTimeMillis();
        var setting = TgTmSetting.of(TgTxOption.ofRTX().label("initMaterialMap"));
        int[] count = { 0 };
        dbManager.execute(setting, () -> {
            count[0] = 0;
            var dao = dbManager.getItemMasterDao();
            try (var stream = dao.selectByType(batchDate, ItemType.RAW_MATERIAL)) {
                stream.forEach(entity -> {
                    materialMap.put(entity.getIId(), entity);
                    count[0]++;
                });
            }
        });
        long end = System.currentTimeMillis();
        LOG.info("select {}.material={} ({}[ms])", ItemMasterDao.TABLE_NAME, count[0], end - start);

        if (materialMap.isEmpty()) {
            throw new RuntimeException("metarial not found in " + ItemMasterDao.TABLE_NAME);
        }
    }

    private void insertCostMaster() {
        var setting = getSetting(CostMasterDao.TABLE_NAME);
        dbManager.execute(setting, () -> {
            CostMasterDao dao = dbManager.getCostMasterDao();
            dao.truncate();
        });

        InitialData03ItemMaster itemMasterData = InitialData03ItemMaster.getDefaultInstance();
        int materialStartId = itemMasterData.getMaterialStartId();
        int materialEndId = itemMasterData.getMaterialEndId();
        CostMasterTask task = new CostMasterTask(materialStartId, materialEndId);
        long start = System.currentTimeMillis();
        executeTask(task);
        joinAllTask();
        long end = System.currentTimeMillis();

        LOG.info("insert {}={} ({}[ms])", CostMasterDao.TABLE_NAME, task.getInsertCount(), end - start);
    }

    @SuppressWarnings("serial")
    private class CostMasterTask extends DaoSplitTask {
        public CostMasterTask(int startId, int endId) {
            super(dbManager, getSetting(CostMasterDao.TABLE_NAME), startId, endId);
        }

        @Override
        protected DaoSplitTask createTask(int startId, int endId) {
            return new CostMasterTask(startId, endId);
        }

        @Override
        protected void execute(int iId, AtomicInteger insertCount) {
            insertCostMaster(iId, dbManager.getItemMasterDao(), dbManager.getCostMasterDao(), insertCount);
        }
    }

    private static final int INIT_COST_FACTORY_PER_MATERIAL = BenchConst.initCostFactoryPerMeterial();
    private static final BigDecimal Q_START = new BigDecimal("1.0");
    private static final BigDecimal Q_END = new BigDecimal("10.0");
    private static final BigDecimal Q_MULT = new BigDecimal("100");
    private static final BigDecimal A_START = new BigDecimal("0.90");
    private static final BigDecimal A_END = new BigDecimal("1.10");
    private static final BigDecimal A_MIN = new BigDecimal("0.01");

    private void insertCostMaster(int materialId, ItemMasterDao itemDao, CostMasterDao dao, AtomicInteger insertCount) {
        ItemMaster materialEntity;
        if (this.materialMap != null) {
            materialEntity = materialMap.get(materialId);
        } else {
            materialEntity = itemDao.selectById(materialId, batchDate);
        }
        if (materialEntity.getIType() != ItemType.RAW_MATERIAL) {
            throw new AssertionError(materialEntity);
        }

        int seed = materialEntity.getIId();

        List<Integer> factoryIds = new ArrayList<>(factoryIdSet);
        int size = INIT_COST_FACTORY_PER_MATERIAL;
        if (size <= 0) {
            size = factoryIdSet.size() / 2; // 50%
        } else if (size > factoryIdSet.size()) {
            size = factoryIdSet.size();
        }
        Set<Integer> factorySet = new TreeSet<>();
        while (factorySet.size() < size) {
            int factoryId = getRandomAndRemove(seed++, factoryIds);
            factorySet.add(factoryId);
        }

        var list = new ArrayList<CostMaster>(factorySet.size());
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

            list.add(entity);
        }
        dao.insertBatch(list);
        insertCount.addAndGet(list.size());
    }
}
