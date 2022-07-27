package com.tsurugidb.benchmark.costaccounting.batch.task;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.math.BigDecimal;
import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import com.tsurugidb.benchmark.costaccounting.batch.task.BenchBatchItemTask.BomNode;
import com.tsurugidb.benchmark.costaccounting.db.doma2.entity.ItemManufacturingMaster;
import com.tsurugidb.benchmark.costaccounting.util.BenchConst;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementUtilTestSupport;
import com.tsurugidb.benchmark.costaccounting.util.MeasurementValue;

class CalculateWeightRatioTest {
    static {
        MeasurementUtilTestSupport.initializeForTest();
    }

    @Test
    void testCalculateWeightRatio0() {
        LocalDate batchDate = LocalDate.of(2020, 10, 6);
        BenchBatchItemTask target = new BenchBatchItemTask(null, batchDate);

        ItemManufacturingMaster entity = new ItemManufacturingMaster();
        entity.setImIId(1);
        BomNode node = target.new BomNode(entity);
        node.weightTotal = new MeasurementValue("mg", BigDecimal.ZERO);

        target.calculateWeightRatio(node, node.weightTotal);

        assertEquals(BigDecimal.ZERO, node.weightRatio);
    }

    @Test
    void testCalculateWeightRatio() {
        LocalDate batchDate = LocalDate.of(2020, 10, 6);
        BenchBatchItemTask target = new BenchBatchItemTask(null, batchDate);

        BomNode root;
        {
            ItemManufacturingMaster entity = new ItemManufacturingMaster();
            entity.setImIId(1);
            root = target.new BomNode(entity);
            root.weightTotal = new MeasurementValue("g", BigDecimal.valueOf(1));
        }
        {
            ItemManufacturingMaster entity = new ItemManufacturingMaster();
            entity.setImIId(2);
            BomNode node = target.new BomNode(entity);
            node.weightTotal = new MeasurementValue("mg", BigDecimal.valueOf(200));
            root.addChild(node);
        }
        {
            ItemManufacturingMaster entity = new ItemManufacturingMaster();
            entity.setImIId(3);
            BomNode node = target.new BomNode(entity);
            node.weightTotal = new MeasurementValue("dg", BigDecimal.valueOf(8));
            root.addChild(node);
        }

        target.calculateWeightRatio(root, root.weightTotal);

        assertEquals(BigDecimal.valueOf(100).setScale(BenchConst.DECIMAL_SCALE), root.weightRatio);
        assertEquals(BigDecimal.valueOf(20).setScale(BenchConst.DECIMAL_SCALE), root.childList.get(0).weightRatio);
        assertEquals(BigDecimal.valueOf(80).setScale(BenchConst.DECIMAL_SCALE), root.childList.get(1).weightRatio);
    }
}
