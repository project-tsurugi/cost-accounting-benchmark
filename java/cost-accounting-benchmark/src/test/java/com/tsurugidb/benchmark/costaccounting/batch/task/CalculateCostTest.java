package com.tsurugidb.benchmark.costaccounting.batch.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;

import org.junit.jupiter.api.Test;
import org.opentest4j.AssertionFailedError;

import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchItemTask.BomNode;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.CostMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemConstructionMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.init.MeasurementUtilTestSupport;
import com.tsurugidb.benchmark.costaccounting.init.MeasurementValue;

class CalculateCostTest {
    static {
        MeasurementUtilTestSupport.initializeForTest();
    }

    @Test
    void testCalculateCost() {
        LocalDate batchDate = LocalDate.of(2020, 10, 6);
        BenchBatchItemTask target = new BenchBatchItemTask(null, batchDate) {
            @Override
            protected CostMaster selectCostMaster(int factoryId, int itemId) {
                CostMaster entity = new CostMaster();
                entity.setCFId(99);
                entity.setCIId(itemId);
                switch (itemId) {
                case 1:
                case 2:
                case 4:
                    return null;
                case 3:
                    entity.setCStockUnit("kg");
                    entity.setCStockQuantity(BigDecimal.valueOf(1));
                    entity.setCStockAmount(BigDecimal.valueOf(900));
                    break;
                default:
                    throw new InternalError(Integer.toString(itemId));
                }
                return entity;
            }

            @Override
            protected ItemMaster selectItemMaster(int itemId) {
                ItemMaster entity = new ItemMaster();
                entity.setIId(itemId);
                switch (itemId) {
                case 1:
                    entity.setIPriceUnit("count");
                    entity.setIPrice(null);
                    break;
                case 2:
                    break;
                case 4:
                    entity.setIPriceUnit("mg");
                    entity.setIPrice(BigDecimal.valueOf(0.5));
                    break;
                default:
                    throw new InternalError(Integer.toString(itemId));
                }
                return entity;
            }
        };

        BomNode root;
        {
            ItemManufacturingMaster entity = new ItemManufacturingMaster();
            entity.setImIId(1);
            entity.setImManufacturingQuantity(BigInteger.valueOf(4000));
            root = target.new BomNode(entity);
            root.requiredQuantity = new MeasurementValue("count", BigDecimal.ZERO);
        }
        {
            ItemConstructionMaster entity = new ItemConstructionMaster();
            entity.setIcIId(2);
            BomNode node = target.new BomNode(entity);
            node.requiredQuantity = new MeasurementValue(null, BigDecimal.ZERO);
            root.addChild(node);

            {
                entity = new ItemConstructionMaster();
                entity.setIcIId(3);
                BomNode child = target.new BomNode(entity);
                child.requiredQuantity = new MeasurementValue("g", BigDecimal.valueOf(300));
                node.addChild(child);
            }
            {
                entity = new ItemConstructionMaster();
                entity.setIcIId(4);
                BomNode child = target.new BomNode(entity);
                child.requiredQuantity = new MeasurementValue("g", BigDecimal.valueOf(400));
                node.addChild(child);
            }
        }

        BigDecimal productionQuantity = new BigDecimal(root.manufactEntity.getImManufacturingQuantity());
        target.calculateCost(root, 99, productionQuantity);

        BomNode node = root.childList.get(0);
        BomNode child1 = node.childList.get(0);
        // 1kg 900yen, 300g
        assertEqualsDecimal(BigDecimal.valueOf(900d / 1000 * 300), child1.totalUnitCost);
        assertEqualsDecimal(child1.totalUnitCost.divide(BigDecimal.valueOf(4000)), child1.unitCost);
        assertEqualsDecimal(child1.totalUnitCost, child1.totalManufacturingCost);
        assertEqualsDecimal(child1.unitCost, child1.manufacturingCost);

        BomNode child2 = node.childList.get(1);
        // 0.5yen/mg, 400g
        assertEqualsDecimal(BigDecimal.valueOf(0.5d * 1000 * 400), child2.totalUnitCost);
        assertEqualsDecimal(child2.totalUnitCost.divide(BigDecimal.valueOf(4000)), child2.unitCost);
        assertEqualsDecimal(child2.totalUnitCost, child2.totalManufacturingCost);
        assertEqualsDecimal(child2.unitCost, child2.manufacturingCost);

        assertEqualsDecimal(BigDecimal.ZERO, node.totalUnitCost);
        assertEqualsDecimal(BigDecimal.ZERO, node.unitCost);
        assertEqualsDecimal(child1.totalManufacturingCost.add(child2.totalManufacturingCost), node.totalManufacturingCost);
        assertEqualsDecimal(node.totalManufacturingCost.divide(BigDecimal.valueOf(4000)), node.manufacturingCost);

        assertEqualsDecimal(BigDecimal.ZERO, root.totalUnitCost);
        assertEqualsDecimal(BigDecimal.ZERO, root.unitCost);
        assertEqualsDecimal(node.totalManufacturingCost, root.totalManufacturingCost);
        assertEqualsDecimal(root.totalManufacturingCost.divide(BigDecimal.valueOf(4000)), root.manufacturingCost);
    }

    private static void assertEqualsDecimal(BigDecimal expected, BigDecimal actual) {
        try {
            assertTrue(expected.compareTo(actual) == 0);
        } catch (AssertionFailedError e) {
            assertEquals(expected, actual);
        }
    }
}
