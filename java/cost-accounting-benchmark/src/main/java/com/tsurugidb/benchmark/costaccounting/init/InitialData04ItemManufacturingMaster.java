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
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchRandom;

public class InitialData04ItemManufacturingMaster extends InitialData {

    public static void main(String... args) throws Exception {
        LocalDate batchDate = DEFAULT_BATCH_DATE;
        int manufacturingSize = BenchConst.initItemManufacturingSize();
        new InitialData04ItemManufacturingMaster(manufacturingSize, batchDate).main();
    }

    private final int manufacturingSize;

    private final List<Integer> factoryIdSet = new ArrayList<>();
    private final Set<Integer> productIdSet = new TreeSet<>();

    public InitialData04ItemManufacturingMaster(int manufacturingSize, LocalDate batchDate) {
        super(batchDate);
        this.manufacturingSize = manufacturingSize;
    }

    private void main() {
        logStart();

        try (CostBenchDbManager manager = initializeDbManager()) {
            initializeField();
            generateItemManufacturingMaster();
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
                Collections.sort(factoryIdSet);
            });
        }
        {
            ItemMasterDao dao = dbManager.getItemMasterDao();
            dbManager.execute(TX_INIT, () -> {
                List<Integer> list = dao.selectIdByType(batchDate, ItemType.PRODUCT);

                productIdSet.clear();
                productIdSet.addAll(list);
            });
        }
    }

    private void generateItemManufacturingMaster() {
        ItemManufacturingMasterDao dao = dbManager.getItemManufacturingMasterDao();

        dbManager.execute(TX_INIT, () -> {
            dao.deleteAll();
            insertItemManufacturingMaster(dao);
        });
    }

    private void insertItemManufacturingMaster(ItemManufacturingMasterDao dao) {
        Set<Integer> remainSet = new TreeSet<>(productIdSet);
        Map<Integer, Set<Integer>> mapA = generateA(remainSet);
        Map<Integer, Set<Integer>> map = generateB(mapA, remainSet);

        map.forEach((factoryId, list) -> {
            for (Integer productId : list) {
                ItemManufacturingMaster entity = newItemManufacturingMaster(factoryId, productId);
                insertItemManufacturingMaster(dao, entity);
            }
        });
    }

    private Map<Integer, Set<Integer>> generateA(Set<Integer> remainSet) {
        Map<Integer, Set<Integer>> mapA = new HashMap<>(factoryIdSet.size());
        for (int factoryId : factoryIdSet) {
            mapA.put(factoryId, new HashSet<>());
        }

        List<Integer> productIds = new ArrayList<>(this.productIdSet);

        // A all
        for (int i = 0; i < 10; i++) {
            int productId = getRandomAndRemove(i, productIds);
            for (Set<Integer> list : mapA.values()) {
                list.add(productId);
            }
            remainSet.remove(productId);
        }

        // A 50%
        for (int i = 0; i < 20; i++) {
            int productId = getRandomAndRemove(i, productIds);
            mapA.forEach((factoryId, list) -> {
                if (factoryId.intValue() % 2 == 1) {
                    list.add(productId);
                }
            });
            remainSet.remove(productId);
        }

        // A 25%
        for (int i = 0; i < 30; i++) {
            int productId = getRandomAndRemove(i, productIds);
            mapA.forEach((factoryId, list) -> {
                if (factoryId.intValue() % 4 == 1) {
                    list.add(productId);
                }
            });
            remainSet.remove(productId);
        }

        // A 10%
        for (int i = 0; i < 40; i++) {
            int productId = getRandomAndRemove(i, productIds);
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
        int seed = 0;
        List<Integer> breadIds = new ArrayList<>(remainSet);
        for (int count = map.values().stream().mapToInt(v -> v.size()).sum(); count < manufacturingSize; count++) {
            int i = random.prandom(count, factoryIdSet.size());
            Integer factoryId = factoryIdSet.get(i);
            Set<Integer> list = map.get(factoryId);

            int breadId = getRandomAndRemove(seed++, breadIds);
            list.add(breadId);
        }

        return map;
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
        private final AtomicInteger amplificationId = new AtomicInteger(1);

        @Override
        protected int getAmplificationId(ItemManufacturingMaster entity) {
            return amplificationId.getAndIncrement();
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

    private void insertItemManufacturingMaster(ItemManufacturingMasterDao dao, ItemManufacturingMaster entity) {
        Collection<ItemManufacturingMaster> list = AMPLIFICATION_ITEM_MANUFACTURING.amplify(entity);
        list.forEach(ent -> {
            dao.insert(ent);
        });
    }

    public static void initializeItemManufacturingMasterRandom(BenchRandom random, ItemManufacturingMaster entity) {
        int seed = entity.getImFId() + entity.getImIId();
        int quantity = random.prandom(seed, 1, 500) * 100;
        entity.setImManufacturingQuantity(BigInteger.valueOf(quantity));
    }
}
