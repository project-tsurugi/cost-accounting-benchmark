package com.tsurugidb.benchmark.costaccounting.init;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.FactoryMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.domain.ItemType;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.init.util.AmplificationRecord;
import com.tsurugidb.benchmark.costaccounting.init.util.DaoTask;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchReproducibleRandom;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;
import com.tsurugidb.iceaxe.transaction.option.TgTxOption;

public class InitialData04ItemManufacturingMaster extends InitialData {

    public static void main(String... args) throws Exception {
        LocalDate batchDate = DEFAULT_BATCH_DATE;
        int manufacturingSize = BenchConst.initItemManufacturingSize();
        new InitialData04ItemManufacturingMaster(manufacturingSize, batchDate).main();
    }

    private final int manufacturingSize;

    private final List<Integer> factoryIdList = new ArrayList<>();
    private final Set<Integer> productIdSet = new TreeSet<>();

    public InitialData04ItemManufacturingMaster(int manufacturingSize, LocalDate batchDate) {
        super(batchDate);
        this.manufacturingSize = manufacturingSize;
    }

    private void main() {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            initializeField();
            var map = generateProductMap();
            insertItemManufacturingMaster(map);
        } finally {
            shutdown();
        }

        dumpExplainCounter(dbManager.getItemMasterDao());

        logEnd();
    }

    private void initializeField() {
        LOG.info("select start");
        {
            FactoryMasterDao dao = dbManager.getFactoryMasterDao();
            var setting = getSetting(() -> TgTxOption.ofRTX());
            dbManager.execute(setting, () -> {
                List<Integer> list = dao.selectAllId();

                factoryIdList.clear();
                factoryIdList.addAll(list);
                Collections.sort(factoryIdList);
            });
        }
        {
            long start = System.currentTimeMillis();
            ItemMasterDao dao = dbManager.getItemMasterDao();
            var setting = getSetting(() -> TgTxOption.ofRTX());
            dbManager.execute(setting, () -> {
                List<Integer> list = dao.selectIdByType(batchDate, ItemType.PRODUCT);

                productIdSet.clear();
                productIdSet.addAll(list);
            });
            long end = System.currentTimeMillis();
            LOG.info("select {}.product={} ({}[ms])", ItemMasterDao.TABLE_NAME, productIdSet.size(), end - start);
        }
    }

    private Map<Integer, Set<Integer>> generateProductMap() {
        LOG.info("generateProductMap start");
        Set<Integer> remainSet = new TreeSet<>(productIdSet);
        Map<Integer, Set<Integer>> mapA = generateA(remainSet);
        Map<Integer, Set<Integer>> map = generateB(mapA, remainSet);
        LOG.info("generateProductMap end");
        return map;
    }

    private Map<Integer, Set<Integer>> generateA(Set<Integer> remainSet) {
        Map<Integer, Set<Integer>> mapA = new HashMap<>(factoryIdList.size());
        for (Integer factoryId : factoryIdList) {
            mapA.put(factoryId, new HashSet<>());
        }

        List<Integer> productIds = new ArrayList<>(this.productIdSet);
        randomShuffle(productIds);
        int productIndex = 0;

        // A all
        for (int i = 0; i < 10; i++) {
            Integer productId = productIds.get(productIndex++);
            for (Set<Integer> list : mapA.values()) {
                list.add(productId);
            }
            remainSet.remove(productId);
        }

        // A 50%
        for (int i = 0; i < 20; i++) {
            Integer productId = productIds.get(productIndex++);
            mapA.forEach((factoryId, list) -> {
                if (factoryId.intValue() % 2 == 1) {
                    list.add(productId);
                }
            });
            remainSet.remove(productId);
        }

        // A 25%
        for (int i = 0; i < 30; i++) {
            Integer productId = productIds.get(productIndex++);
            mapA.forEach((factoryId, list) -> {
                if (factoryId.intValue() % 4 == 1) {
                    list.add(productId);
                }
            });
            remainSet.remove(productId);
        }

        // A 10%
        for (int i = 0; i < 40; i++) {
            Integer productId = productIds.get(productIndex++);
            mapA.forEach((factoryId, list) -> {
                if (factoryId.intValue() % 10 == 1) {
                    list.add(productId);
                }
            });
            remainSet.remove(productId);
        }

        return mapA;
    }

    private Map<Integer, Set<Integer>> generateB(Map<Integer, Set<Integer>> map, Set<Integer> remainSet) {
        int startCount = map.values().stream().mapToInt(v -> v.size()).sum();
        LOG.info("generateB start={}, target={}, remainSet={}", startCount, manufacturingSize, remainSet.size());

        List<Integer> breadIds = new ArrayList<>(remainSet);
        randomShuffle(breadIds);
        int breadIndex = 0;

        for (int count = startCount; count < manufacturingSize; count++) {
            int i = random.prandom(count, factoryIdList.size());
            Integer factoryId = factoryIdList.get(i);
            Set<Integer> set = map.get(factoryId);

            Integer breadId = breadIds.get(breadIndex++);
            set.add(breadId);
        }

        return map;
    }

    private void insertItemManufacturingMaster(Map<Integer, Set<Integer>> map) {
        ItemManufacturingMasterDao dao = dbManager.getItemManufacturingMasterDao();

        var setting = getSetting(ItemManufacturingMasterDao.TABLE_NAME);
        LOG.info("truncate start");
        dbManager.execute(setting, () -> {
            dao.truncate();
        });
        LOG.info("truncate end, insert start");

        var taskList = new ArrayList<InsertItemManufacturingMasterTask>(map.size());
        long start = System.currentTimeMillis();
        map.forEach((factoryId, list) -> {
            var task = new InsertItemManufacturingMasterTask(setting, factoryId, list);
            executeTask(task);
            taskList.add(task);
        });
        joinAllTask();
        long end = System.currentTimeMillis();

        int insertCount = 0;
        for (var task : taskList) {
            insertCount += task.getInsertCount();
        }
        LOG.info("insert {}={} ({}[ms])", ItemManufacturingMasterDao.TABLE_NAME, insertCount, end - start);
    }

    @SuppressWarnings("serial")
    private class InsertItemManufacturingMasterTask extends DaoTask {

        private final int factoryId;
        private final Set<Integer> productIds;

        public InsertItemManufacturingMasterTask(TgTmSetting setting, int factoryId, Set<Integer> productIds) {
            super(dbManager, setting);
            this.factoryId = factoryId;
            this.productIds = productIds;
        }

        @Override
        protected void execute(AtomicInteger insertCount) {
            ItemManufacturingMasterDao dao = dbManager.getItemManufacturingMasterDao();

            for (int productId : productIds) {
                ItemManufacturingMaster entity = newItemManufacturingMaster(factoryId, productId);
                insertItemManufacturingMaster(dao, entity, insertCount);
            }
        }
    }

    public ItemManufacturingMaster newItemManufacturingMaster(int factoryId, int productId) {
        ItemManufacturingMaster entity = new ItemManufacturingMaster();

        entity.setImFId(factoryId);
        entity.setImIId(productId);
        initializeStartEndDate(factoryId + productId, entity);
        initializeItemManufacturingMasterRandom(random, entity);

        return entity;
    }

    // 1.6倍に増幅する
    private final AmplificationRecord<ItemManufacturingMaster> AMPLIFICATION_ITEM_MANUFACTURING = new AmplificationRecord<ItemManufacturingMaster>(1.6, random) {

        @Override
        protected int getAmplificationId(ItemManufacturingMaster entity) {
            return entity.getImFId() + entity.getImIId();
        }

        @Override
        protected int getSeed(ItemManufacturingMaster entity) {
            return entity.getImFId() + entity.getImIId();
        }

        @Override
        protected ItemManufacturingMaster getClone(ItemManufacturingMaster entity) {
            return entity.clone();
        }

        @Override
        protected void initialize(ItemManufacturingMaster entity) {
            initializeItemManufacturingMasterRandom(random, entity);
        }
    };

    private void insertItemManufacturingMaster(ItemManufacturingMasterDao dao, ItemManufacturingMaster entity, AtomicInteger insertCount) {
        Collection<ItemManufacturingMaster> list = AMPLIFICATION_ITEM_MANUFACTURING.amplify(entity);
        dao.insertBatch(list);
        insertCount.addAndGet(list.size());
    }

    public static void initializeItemManufacturingMasterRandom(BenchReproducibleRandom random, ItemManufacturingMaster entity) {
        int seed = entity.getImFId() + entity.getImIId();
        int quantity = random.prandom(seed, 1, 500) * 100;
        entity.setImManufacturingQuantity(BigInteger.valueOf(quantity));
    }
}
