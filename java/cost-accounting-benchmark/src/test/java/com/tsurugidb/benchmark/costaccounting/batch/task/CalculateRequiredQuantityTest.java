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
package com.tsurugidb.benchmark.costaccounting.batch.task;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.math.RoundingMode;
import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchItemTask.BomNode;
import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchItemTask.Ratio;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemConstructionMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.db.entity.ItemMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtilTestSupport;

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
