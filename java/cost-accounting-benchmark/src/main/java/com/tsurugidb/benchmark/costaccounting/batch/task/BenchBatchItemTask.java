package com.tsurugidb.benchmark.costaccounting.batch.task;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.text.MessageFormat;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.tsurugidb.benchmark.costaccounting.db.CostBenchDbManager;
import com.tsurugidb.benchmark.costaccounting.db.dao.CostMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemConstructionMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemManufacturingMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ItemMasterDao;
import com.tsurugidb.benchmark.costaccounting.db.dao.ResultTableDao;
import com.tsurugidb.benchmark.costaccounting.db.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ResultTable;
import com.tsurugidb.benchmark.costaccounting.db.jdbc.CostBenchDbManagerJdbc;
import com.tsurugidb.benchmark.costaccounting.init.InitialData;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst.IsolationLevel;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtil;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtil.ValuePair;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementValue;
import com.tsurugidb.iceaxe.transaction.TgTxOption;
import com.tsurugidb.iceaxe.transaction.manager.TgTmSetting;

public class BenchBatchItemTask {
    private static final Logger LOG = LoggerFactory.getLogger(BenchBatchItemTask.class);

    private final CostBenchDbManager dbManager;
    private final LocalDate batchDate;

    public BenchBatchItemTask(CostBenchDbManager dbManager, LocalDate batchDate) {
        this.dbManager = dbManager;
        this.batchDate = batchDate;
    }

    public void execute(ItemManufacturingMaster manufact) {
        if (manufact.getImManufacturingQuantity().compareTo(BigInteger.ZERO) == 0) {
            return;
        }

        int factoryId = manufact.getImFId();
        int productId = manufact.getImIId();
        BomNode node = selectBomTree(manufact);
        // node.debugDump();

        calculateWeight(node);
        calculateWeightRatio(node, node.weightTotal);

        BigDecimal manufacturingQuantity = new BigDecimal(manufact.getImManufacturingQuantity());
        calculateRequiredQuantity(node, new Ratio(BigDecimal.ONE, BigDecimal.ONE), manufacturingQuantity);

        calculateCost(node, factoryId, manufacturingQuantity);

        switch (4) {
        case 1:
            insertResult(node, factoryId, productId, manufact.getImManufacturingQuantity());
            break;
        case 2:
            insertResultBatch(node, factoryId, productId, manufact.getImManufacturingQuantity());
            break;
        case 3:
            insertResult2(node, factoryId, productId, manufact.getImManufacturingQuantity());
            break;
        case 4:
            insertResultBatch2(node, factoryId, productId, manufact.getImManufacturingQuantity());
            break;
        default:
            throw new InternalError();
        }
    }

    protected class BomNode {
        public final int itemId;
        public final ItemManufacturingMaster manufactEntity;
        public final ItemConstructionMaster constructEntity;
        private BomNode parentNode;
        public final List<BomNode> childList = new ArrayList<>();

        private ItemMaster itemEntity;

        public MeasurementValue weight;
        public MeasurementValue weightTotal;
        public BigDecimal weightRatio;

        public MeasurementValue standardQuantity;
        public MeasurementValue requiredQuantity;

        public BigDecimal unitCost;
        public BigDecimal totalUnitCost;
        public BigDecimal manufacturingCost;
        public BigDecimal totalManufacturingCost;

        public BomNode(ItemManufacturingMaster entity) {
            this.itemId = entity.getImIId();
            this.manufactEntity = entity;
            this.constructEntity = null;
        }

        public BomNode(ItemConstructionMaster entity) {
            this.itemId = entity.getIcIId();
            this.manufactEntity = null;
            this.constructEntity = entity;
        }

        public void addChild(BomNode node) {
            node.parentNode = this;
            childList.add(node);
        }

        public ItemMaster getItemMasterEntity() {
            if (itemEntity == null) {
                this.itemEntity = selectItemMaster(itemId);
                if (itemEntity == null) {
                    throw new RuntimeException(MessageFormat.format("not found item_master. itemId={0}, batchDate={1}", itemId, batchDate));
                }
            }
            return itemEntity;
        }

        public int productId() {
            if (manufactEntity != null) {
                return manufactEntity.getImIId();
            }
            return parentNode.productId();
        }

        public boolean equalsById(BomNode that) {
            if (this.itemId != that.itemId) {
                return false;
            }

            if (childList.size() != that.childList.size()) {
                return false;
            }
            for (BomNode child : childList) {
                if (!that.childList.stream().anyMatch(c -> c.equalsById(child))) {
                    return false;
                }
            }

            return true;
        }

        public void debugDump(String title, Exception e) {
            LOG.error("{} error\n" //
                    + "node.itemId={}\n" //
                    + "node.manufactEntity={}\n" //
                    + "node.constructEntity={}\n" //
                    + "node.itemEntity={}\n" //
                    + "node.standardQuantity={}\n" //
                    + "node.requiredQuantity={}\n" //
                    + "node.childList={}", //
                    title, //
                    this.itemId, //
                    this.manufactEntity, //
                    this.constructEntity, //
                    this.itemEntity, //
                    this.standardQuantity, //
                    this.requiredQuantity, //
                    childList.stream().map(node -> Integer.toString(node.itemId)).collect(Collectors.joining(",")), //
                    e);
//			for (BomNode child : childList) {
//				child.debugDump();
//			}
        }
    }

    protected BomNode selectBomTree(ItemManufacturingMaster manufact) {
        BomNode root = new BomNode(manufact);
        switch (1) {
        case 1:
            createBomTree1(root);
            break;
        case 2:
            createBomTree2(root);
            break;
        default:
            throw new InternalError();
        }
        return root;
    }

    protected void createBomTree1(BomNode parentNode) {
        createBomTree(parentNode, itemId -> selectItemConstructionMaster(itemId, batchDate));
    }

    protected List<ItemConstructionMaster> selectItemConstructionMaster(int parentItemId, LocalDate batchDate) {
        ItemConstructionMasterDao dao = dbManager.getItemConstructionMasterDao();
        return dao.selectByParentId(parentItemId, batchDate);
    }

    protected void createBomTree2(BomNode parentNode) {
        Map<Integer, List<ItemConstructionMaster>> map = new HashMap<>();
        try (Stream<ItemConstructionMaster> stream = selectItemConstructionMasterRecursive(parentNode.itemId, batchDate)) {
            stream.forEach(entity -> {
                List<ItemConstructionMaster> list = map.computeIfAbsent(entity.getIcParentIId(), k -> new ArrayList<>());
                list.add(entity);
            });
        }

        createBomTree(parentNode, itemId -> map.getOrDefault(itemId, Collections.emptyList()));
    }

    protected Stream<ItemConstructionMaster> selectItemConstructionMasterRecursive(int parentItemId, LocalDate batchDate) {
        ItemConstructionMasterDao dao = dbManager.getItemConstructionMasterDao();
        return dao.selectRecursiveByParentId(parentItemId, batchDate);
    }

    protected void createBomTree(BomNode parentNode, Function<Integer, List<ItemConstructionMaster>> f) {
        int itemId = parentNode.itemId;
        List<ItemConstructionMaster> list = f.apply(itemId);
        for (ItemConstructionMaster entity : list) {
            BomNode node = new BomNode(entity);
            parentNode.addChild(node);

            createBomTree(node, f);
        }
    }

    protected ItemMaster selectItemMaster(int itemId) {
        ItemMasterDao dao = dbManager.getItemMasterDao();
        return dao.selectById(itemId, batchDate);
    }

    void calculateWeight(BomNode node) {
        try {
            calculateWeight1(node);
        } catch (RuntimeException e) {
            if (!dbManager.isRetriable(e)) {
                node.debugDump("calculateWeight", e);
            }
            throw e;
        }
        calculateWeight2(node);
    }

    protected void calculateWeight1(BomNode node) {
        ItemConstructionMaster entity = node.constructEntity;
        if (entity == null || entity.getIcMaterialQuantity() == null) {
            node.weight = new MeasurementValue("mg", BigDecimal.ZERO);
        } else {
            if (MeasurementUtil.isWeight(entity.getIcMaterialUnit())) {
                node.weight = new MeasurementValue(entity.getIcMaterialUnit(), entity.getIcMaterialQuantity());
            } else {
                ItemMaster itemEntity = node.getItemMasterEntity();
                BigDecimal quantity = MeasurementUtil.convertUnit(entity.getIcMaterialQuantity(), entity.getIcMaterialUnit(), itemEntity.getIUnit());
                node.weight = new MeasurementValue(itemEntity.getIWeightUnit(), quantity.multiply(itemEntity.getIWeightRatio()));
            }
        }
    }

    protected void calculateWeight2(BomNode node) {
        MeasurementValue weightTotal = node.weight;
        for (BomNode child : node.childList) {
            calculateWeight(child);

            MeasurementValue childWeight = child.weightTotal;
            weightTotal = weightTotal.add(childWeight);
        }
        node.weightTotal = weightTotal;
    }

    protected void calculateWeightRatio(BomNode node, MeasurementValue rootWeightTotal) {
        try {
            calculateWeightRatio1(node, rootWeightTotal);
        } catch (RuntimeException e) {
            if (!dbManager.isRetriable(e)) {
                node.debugDump("calculateWeightRatio", e);
            }
            throw e;
        }
        calculateWeightRatio2(node, rootWeightTotal);
    }

    private static final BigDecimal D_100 = BigDecimal.valueOf(100);

    protected void calculateWeightRatio1(BomNode node, MeasurementValue rootWeightTotal) {
        if (node.weightTotal.value.compareTo(BigDecimal.ZERO) == 0) {
            node.weightRatio = BigDecimal.ZERO;
        } else {
            MeasurementValue weightTotal = new MeasurementValue(node.weightTotal.unit, node.weightTotal.value.multiply(D_100));
            node.weightRatio = weightTotal.divide(rootWeightTotal, RoundingMode.DOWN);
        }
    }

    protected void calculateWeightRatio2(BomNode node, MeasurementValue rootWeightTotal) {
        for (BomNode child : node.childList) {
            calculateWeightRatio(child, rootWeightTotal);
        }
    }

    void calculateRequiredQuantity(BomNode node, Ratio parentRatio, BigDecimal manufacturingQuantity) {
        Ratio ratio;
        try {
            ratio = calculateRequiredQuantity1(node, parentRatio, manufacturingQuantity);
        } catch (RuntimeException e) {
            if (!dbManager.isRetriable(e)) {
                node.debugDump("calculateRequiredQuantity", e);
            }
            throw e;
        }
        calculateRequiredQuantity2(node, ratio, manufacturingQuantity);
    }

    protected static class Ratio {
        public final BigDecimal numerator;
        public final BigDecimal denominator;

        public Ratio(BigDecimal numerator, BigDecimal denominator) {
            this.numerator = numerator;
            this.denominator = denominator;
        }

        public Ratio multiply(BigDecimal numerator, BigDecimal denominator) {
            BigDecimal n = this.numerator.multiply(numerator);
            BigDecimal d = this.denominator.multiply(denominator);
            return new Ratio(n, d);
        }

        @Override
        public String toString() {
            return numerator + "/" + denominator + "(" + numerator.divide(denominator, BenchConst.DECIMAL_SCALE, RoundingMode.HALF_UP) + ")";
        }
    }

    protected Ratio calculateRequiredQuantity1(BomNode node, Ratio parentRatio, BigDecimal manufacturingQuantity) {
        ItemConstructionMaster construct = node.constructEntity;

        Ratio ratio;
        if (construct != null) {
            ratio = parentRatio.multiply(D_100, D_100.subtract(construct.getIcLossRatio()));
        } else {
            ratio = parentRatio;
        }

        if (construct == null || construct.getIcMaterialQuantity() == null) {
            String unit = node.getItemMasterEntity().getIUnit();
            node.standardQuantity = new MeasurementValue(unit, BigDecimal.ZERO);
        } else {
            BigDecimal quantity = construct.getIcMaterialQuantity().multiply(ratio.numerator).divide(ratio.denominator, BenchConst.DECIMAL_SCALE, RoundingMode.DOWN);
            node.standardQuantity = new MeasurementValue(construct.getIcMaterialUnit(), quantity);
        }

        String requiredUnit;
        switch (MeasurementUtil.getType(node.standardQuantity.unit)) {
        case CAPACITY:
            requiredUnit = "L";
            break;
        case WEIGHT:
            requiredUnit = "kg";
            break;
        default:
            requiredUnit = node.standardQuantity.unit;
            break;
        }
        node.requiredQuantity = node.standardQuantity.multiply(manufacturingQuantity).convertUnit(requiredUnit);

        return ratio;
    }

    protected void calculateRequiredQuantity2(BomNode node, Ratio ratio, BigDecimal manufacturingQuantity) {
        for (BomNode child : node.childList) {
            calculateRequiredQuantity(child, ratio, manufacturingQuantity);
        }
    }

    void calculateCost(BomNode node, int factoryId, BigDecimal manufacturingQuantity) {
        try {
            calculateCost1(node, factoryId, manufacturingQuantity);
        } catch (RuntimeException e) {
            if (!dbManager.isRetriable(e)) {
                node.debugDump("calculateCost", e);
            }
            throw e;
        }
        calculateCost2(node, factoryId, manufacturingQuantity);
    }

    protected void calculateCost1(BomNode node, int factoryId, BigDecimal manufacturingQuantity) {
        CostMaster costEntity = selectCostMaster(factoryId, node.itemId);
        if (costEntity != null) {
            MeasurementValue stock = new MeasurementValue(costEntity.getCStockUnit(), costEntity.getCStockQuantity());
            ValuePair c = MeasurementUtil.getCommonUnitValue(stock, node.requiredQuantity);
            node.totalUnitCost = costEntity.getCStockAmount().multiply(c.value2).divide(c.value1, BenchConst.DECIMAL_SCALE, RoundingMode.DOWN);
        } else {
            ItemMaster itemEntity = node.getItemMasterEntity();
            if (itemEntity.getIPrice() == null) {
                node.totalUnitCost = BigDecimal.ZERO;
            } else {
                String commonUnit = MeasurementUtil.getCommonUnit(itemEntity.getIPriceUnit(), node.requiredQuantity.unit);
                BigDecimal price = MeasurementUtil.convertPriceUnit(itemEntity.getIPrice(), itemEntity.getIPriceUnit(), commonUnit);
                BigDecimal required = MeasurementUtil.convertUnit(node.requiredQuantity.value, node.requiredQuantity.unit, commonUnit);
                node.totalUnitCost = price.multiply(required);
            }
        }

        node.unitCost = node.totalUnitCost.divide(manufacturingQuantity, BenchConst.DECIMAL_SCALE, RoundingMode.DOWN);
    }

    protected void calculateCost2(BomNode node, int factoryId, BigDecimal manufacturingQuantity) {
        BigDecimal totalManufacturingCost = node.totalUnitCost;
        for (BomNode child : node.childList) {
            calculateCost(child, factoryId, manufacturingQuantity);

            totalManufacturingCost = totalManufacturingCost.add(child.totalManufacturingCost);
        }
        node.totalManufacturingCost = totalManufacturingCost;

        node.manufacturingCost = totalManufacturingCost.divide(manufacturingQuantity, BenchConst.DECIMAL_SCALE, RoundingMode.DOWN);
    }

    protected CostMaster selectCostMaster(int factoryId, int itemId) {
        CostMasterDao dao = dbManager.getCostMasterDao();
        return dao.selectById(factoryId, itemId);
    }

    protected void insertResult(BomNode node, int factoryId, int productId, BigInteger manufacturingQuantity) {
        Collection<ResultTable> list = createResults(node, factoryId, productId, manufacturingQuantity);
        for (ResultTable entity : list) {
            insertResultTable(entity);
        }
    }

    protected void insertResultTable(ResultTable entity) {
        ResultTableDao dao = dbManager.getResultTableDao();
        dao.insert(entity);
    }

    protected void insertResultBatch(BomNode node, int factoryId, int productId, BigInteger manufacturingQuantity) {
        Collection<ResultTable> list = createResults(node, factoryId, productId, manufacturingQuantity);
        insertResultTable(list);
    }

    protected void insertResultTable(Collection<ResultTable> list) {
        ResultTableDao dao = dbManager.getResultTableDao();
        dao.insertBatch(list);
    }

    protected Collection<ResultTable> createResults(BomNode node, int factoryId, int productId, BigInteger manufacturingQuantity) {
        List<ResultTable> list = new ArrayList<>();
        createResults(list, node, factoryId, productId, manufacturingQuantity);

        Map<List<Object>, ResultTable> map = new LinkedHashMap<>(list.size());
        for (ResultTable entity : list) {
            List<Object> key = Arrays.asList(entity.getRParentIId(), entity.getRIId());
            ResultTable exists = map.get(key);
            if (exists == null) {
                map.put(key, entity);
            } else {
                addResult(exists, entity);
            }
        }

        return map.values();
    }

    protected void createResults(List<ResultTable> result, BomNode node, int factoryId, int productId, BigInteger manufacturingQuantity) {
        ResultTable entity = createResult(node, factoryId, productId, manufacturingQuantity);
        result.add(entity);

        for (BomNode child : node.childList) {
            createResults(result, child, factoryId, productId, manufacturingQuantity);
        }
    }

    protected void insertResult2(BomNode node, int factoryId, int productId, BigInteger manufacturingQuantity) {
        Map<Object, BomNode> map = new HashMap<>();
        aggregateBomNode(map, node);
        for (BomNode n : map.values()) {
            ResultTable entity = createResult(n, factoryId, productId, manufacturingQuantity);
            insertResultTable(entity);
        }
    }

    protected void insertResultBatch2(BomNode node, int factoryId, int productId, BigInteger manufacturingQuantity) {
        Map<Object, BomNode> map = new HashMap<>();
        aggregateBomNode(map, node);
        List<ResultTable> list = new ArrayList<>(map.size());
        for (BomNode n : map.values()) {
            ResultTable entity = createResult(n, factoryId, productId, manufacturingQuantity);
            list.add(entity);
        }
        insertResultTable(list);
    }

    private void aggregateBomNode(Map<Object, BomNode> map, BomNode node) {
        int parentId = (node.constructEntity != null) ? node.constructEntity.getIcParentIId() : 0;
        List<Integer> key = Arrays.asList(parentId, node.itemId);
        BomNode before = map.get(key);
        if (before == null) {
            map.put(key, node);
        } else {
            addNode(before, node);
        }

        for (BomNode child : node.childList) {
            aggregateBomNode(map, child);
        }
    }

    protected ResultTable createResult(BomNode node, int factoryId, int productId, BigInteger manufacturingQuantity) {
        ResultTable entity = new ResultTable();

        entity.setRFId(factoryId);
        entity.setRManufacturingDate(batchDate);
        entity.setRProductIId(productId);
        if (node.constructEntity != null) {
            entity.setRParentIId(node.constructEntity.getIcParentIId());
        } else {
            entity.setRParentIId(0);
        }
        entity.setRIId(node.itemId);

        entity.setRManufacturingQuantity(manufacturingQuantity);

        entity.setRWeightUnit(node.weight.unit);
        entity.setRWeight(node.weight.value);
        entity.setRWeightTotalUnit(node.weightTotal.unit);
        entity.setRWeightTotal(node.weightTotal.value);
        entity.setRWeightRatio(node.weightRatio);

        entity.setRStandardQuantityUnit(node.standardQuantity.unit);
        entity.setRStandardQuantity(node.standardQuantity.value.setScale(4, RoundingMode.DOWN));
        entity.setRRequiredQuantityUnit(node.requiredQuantity.unit);
        entity.setRRequiredQuantity(node.requiredQuantity.value.setScale(4, RoundingMode.DOWN));

        entity.setRUnitCost(node.unitCost.setScale(2, RoundingMode.DOWN));
        entity.setRTotalUnitCost(node.totalUnitCost.setScale(2, RoundingMode.DOWN));
        entity.setRManufacturingCost(node.manufacturingCost.setScale(2, RoundingMode.DOWN));
        entity.setRTotalManufacturingCost(node.totalManufacturingCost.setScale(2, RoundingMode.DOWN));

        return entity;
    }

    protected void addResult(ResultTable entity, ResultTable right) {
        if (!entity.getRWeightUnit().equals(right.getRWeightUnit())) {
            throw new RuntimeException();
        }
        entity.setRWeight(entity.getRWeight().add(right.getRWeight()));

        if (!entity.getRWeightTotalUnit().equals(right.getRWeightTotalUnit())) {
            throw new RuntimeException();
        }
        entity.setRWeightTotal(entity.getRWeightTotal().add(right.getRWeightTotal()));

        entity.setRWeightRatio(entity.getRWeightRatio().add(right.getRWeightRatio()));

        if (!entity.getRStandardQuantityUnit().equals(right.getRStandardQuantityUnit())) {
            throw new RuntimeException();
        }
        entity.setRStandardQuantity(entity.getRStandardQuantity().add(right.getRStandardQuantity()));

        if (!entity.getRRequiredQuantityUnit().equals(right.getRRequiredQuantityUnit())) {
            throw new RuntimeException();
        }
        entity.setRRequiredQuantity(entity.getRRequiredQuantity().add(right.getRRequiredQuantity()));

        entity.setRUnitCost(entity.getRUnitCost().add(right.getRUnitCost()));
        entity.setRTotalUnitCost(entity.getRTotalUnitCost().add(right.getRTotalUnitCost()));
        entity.setRManufacturingCost(entity.getRManufacturingCost().add(right.getRManufacturingCost()));
        entity.setRTotalManufacturingCost(entity.getRTotalManufacturingCost().add(right.getRTotalManufacturingCost()));
    }

    private void addNode(BomNode node, BomNode right) {
        node.weight = node.weight.add(right.weight);
        node.weightTotal = node.weightTotal.add(right.weightTotal);
        node.weightRatio = node.weightRatio.add(right.weightRatio);

        node.standardQuantity = node.standardQuantity.add(right.standardQuantity);
        node.requiredQuantity = node.requiredQuantity.add(right.requiredQuantity);

        node.unitCost = node.unitCost.add(right.unitCost);
        node.totalUnitCost = node.totalUnitCost.add(right.totalUnitCost);
        node.manufacturingCost = node.manufacturingCost.add(right.manufacturingCost);
        node.totalManufacturingCost = node.totalManufacturingCost.add(right.totalManufacturingCost);
    }

    // TODO remove test method
    // for test
    public static void main(String[] args) {
        test1();
//      test2();
    }

    static void test1() {
        CostBenchDbManager manager = new CostBenchDbManagerJdbc(IsolationLevel.READ_COMMITTED);
        BenchBatchItemTask task = new BenchBatchItemTask(manager, InitialData.DEFAULT_BATCH_DATE);

        var setting = TgTmSetting.of(TgTxOption.ofRTX());
        manager.execute(setting, () -> {
            ItemManufacturingMasterDao itemManufacturingMasterDao = manager.getItemManufacturingMasterDao();
            ItemManufacturingMaster manufact = itemManufacturingMasterDao.selectById(1, 67586, InitialData.DEFAULT_BATCH_DATE);
            BomNode root1 = task.new BomNode(manufact);
            {
                long s = System.currentTimeMillis();
                task.createBomTree1(root1);
                long e = System.currentTimeMillis();
                LOG.info("1: %d", e - s);
            }
            BomNode root2 = task.new BomNode(manufact);
            {
                long s = System.currentTimeMillis();
                task.createBomTree2(root2);
                long e = System.currentTimeMillis();
                LOG.info("2: {}", e - s);
            }

            LOG.info("equals=", root1.equalsById(root2));
        });
    }

    static void test2() {
        int itemId = 11649;
        LocalDate date = InitialData.DEFAULT_BATCH_DATE;

        CostBenchDbManager manager = new CostBenchDbManagerJdbc(IsolationLevel.READ_COMMITTED);
        BenchBatchItemTask task = new BenchBatchItemTask(manager, date);

        var setting = TgTmSetting.of(TgTxOption.ofLTX(ResultTableDao.TABLE_NAME));
        manager.execute(setting, () -> {
            ResultTableDao resultTableDao = manager.getResultTableDao();
            resultTableDao.deleteByProductId(1, date, itemId);

            ItemManufacturingMasterDao itemManufacturingMasterDao = manager.getItemManufacturingMasterDao();
            ItemManufacturingMaster manufact = itemManufacturingMasterDao.selectById(1, itemId, date);

            task.execute(manufact);
        });
    }
}
