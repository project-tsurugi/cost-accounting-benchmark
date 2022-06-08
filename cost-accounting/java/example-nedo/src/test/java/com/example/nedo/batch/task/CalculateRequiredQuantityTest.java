package com.example.nedo.batch.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import com.example.nedo.BenchConst;
import com.example.nedo.batch.task.BenchBatchItemTask.BomNode;
import com.example.nedo.batch.task.BenchBatchItemTask.Ratio;
import com.example.nedo.db.doma2.entity.ItemConstructionMaster;
import com.example.nedo.db.doma2.entity.ItemManufacturingMaster;
import com.example.nedo.db.doma2.entity.ItemMaster;
import com.example.nedo.init.MeasurementUtilTestSupport;

class CalculateRequiredQuantityTest {
    static {
        MeasurementUtilTestSupport.initializeForTest();
    }

    @Test
    void testCalculateRequiredQuantity() {
        LocalDate batchDate = LocalDate.of(2020, 10, 6);
        BenchBatchItemTask target = new BenchBatchItemTask(null, batchDate) {
            @Override
            protected ItemMaster selectItemMaster(int itemId) {
                ItemMaster entity = new ItemMaster();
                entity.setIId(itemId);
                switch (itemId) {
                case 1:
                    entity.setIUnit("count");
                    break;
                case 2:
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
        }
        {
            ItemConstructionMaster entity = new ItemConstructionMaster();
            entity.setIcIId(2);
            entity.setIcLossRatio(BigDecimal.valueOf(50));
            entity.setIcMaterialQuantity(null);
            entity.setIcMaterialUnit(null);
            BomNode node = target.new BomNode(entity);
            root.addChild(node);

            {
                entity = new ItemConstructionMaster();
                entity.setIcIId(3);
                entity.setIcLossRatio(BigDecimal.valueOf(80));
                entity.setIcMaterialQuantity(BigDecimal.valueOf(3));
                entity.setIcMaterialUnit("mg");
                BomNode child = target.new BomNode(entity);
                node.addChild(child);
            }
        }

        Ratio rate = new Ratio(BigDecimal.ONE, BigDecimal.ONE);
        BigDecimal productionQuantity = new BigDecimal(root.manufactEntity.getImManufacturingQuantity());
        target.calculateRequiredQuantity(root, rate, productionQuantity);

        assertEquals(BigDecimal.ZERO, root.standardQuantity.value);
        assertEquals("count", root.standardQuantity.unit);
        assertEquals(BigDecimal.ZERO, root.requiredQuantity.value);
        assertEquals("count", root.requiredQuantity.unit);
        {
            BomNode node = root.childList.get(0);
            assertEquals(BigDecimal.ZERO, node.standardQuantity.value);
            assertNull(node.standardQuantity.unit);
            assertEquals(BigDecimal.ZERO, node.requiredQuantity.value);
            assertNull(node.requiredQuantity.unit);
            {
                BomNode child = node.childList.get(0);
                assertEquals("mg", child.standardQuantity.unit);
                assertEquals(BigDecimal.valueOf(30).setScale(BenchConst.DECIMAL_SCALE), child.standardQuantity.value);
                assertEquals("kg", child.requiredQuantity.unit);
                assertEquals(BigDecimal.valueOf(3 * (100 / (100 - 50)) * (100 / (100 - 80)) * 4000).divide(BigDecimal.valueOf(1000_000), BenchConst.DECIMAL_SCALE, RoundingMode.DOWN),
                        child.requiredQuantity.value);
            }
        }
    }
}
