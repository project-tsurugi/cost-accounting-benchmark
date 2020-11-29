package com.example.nedo.batch;

import static org.junit.jupiter.api.Assertions.*;

import java.math.BigDecimal;
import java.time.LocalDate;

import org.junit.jupiter.api.Test;

import com.example.nedo.BenchConst;
import com.example.nedo.batch.BenchBatchItemTask.BomNode;
import com.example.nedo.init.MeasurementUtilTestSupport;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

class CalculateWeightTest {
	static {
		MeasurementUtilTestSupport.initializeForTest();
	}

	@Test
	void testCalculateWeight0() {
		LocalDate batchDate = LocalDate.of(2020, 10, 6);
		BenchBatchItemTask target = new BenchBatchItemTask(batchDate);

		ItemManufacturingMaster entity = new ItemManufacturingMaster();
		entity.setImIId(1);
		BomNode node = target.new BomNode(entity);

		target.calculateWeight(node);

		assertEquals(BigDecimal.ZERO, node.weight.value);
		assertEquals("mg", node.weight.unit);
		assertEquals(node.weight, node.weightTotal);
	}

	@Test
	void testCalculateWeight_MaterialVolume_null() {
		LocalDate batchDate = LocalDate.of(2020, 10, 6);
		BenchBatchItemTask target = new BenchBatchItemTask(batchDate);

		ItemConstructionMaster entity = new ItemConstructionMaster();
		entity.setIcIId(1);
		BomNode node = target.new BomNode(entity);

		target.calculateWeight(node);

		assertEquals(BigDecimal.ZERO, node.weight.value);
		assertEquals("mg", node.weight.unit);
		assertEquals(node.weight, node.weightTotal);
	}

	@Test
	void testCalculateWeight_MaterialUnit_weight() {
		LocalDate batchDate = LocalDate.of(2020, 10, 6);
		BenchBatchItemTask target = new BenchBatchItemTask(batchDate);

		ItemConstructionMaster entity = new ItemConstructionMaster();
		entity.setIcIId(1);
		entity.setIcMaterialUnit("g");
		entity.setIcMaterialQuantity(BigDecimal.ONE);
		BomNode node = target.new BomNode(entity);

		target.calculateWeight(node);

		assertEquals(BigDecimal.ONE, node.weight.value);
		assertEquals("g", node.weight.unit);
		assertEquals(node.weight, node.weightTotal);
	}

	@Test
	void testCalculateWeight_MaterialUnit_notWeight() {
		LocalDate batchDate = LocalDate.of(2020, 10, 6);
		BenchBatchItemTask target = new BenchBatchItemTask(batchDate) {
			@Override
			protected ItemMaster selectItemMaster(int itemId) {
				assertEquals(1, itemId);

				ItemMaster entity = new ItemMaster();
				entity.setIId(itemId);
				entity.setIUnit("mL");
				entity.setIWeightRatio(new BigDecimal("1.1"));
				entity.setIWeightUnit("g");
				return entity;
			}
		};

		ItemConstructionMaster entity = new ItemConstructionMaster();
		entity.setIcIId(1);
		entity.setIcMaterialUnit("cL");
		entity.setIcMaterialQuantity(BigDecimal.ONE);
		BomNode node = target.new BomNode(entity);

		target.calculateWeight(node);

		assertEquals(new BigDecimal("11.0").setScale(BenchConst.DECIMAL_SCALE + 1), node.weight.value);
		assertEquals("g", node.weight.unit);
		assertEquals(node.weight, node.weightTotal);
	}

	@Test
	void testCalculateWeight_child() {
		LocalDate batchDate = LocalDate.of(2020, 10, 6);
		BenchBatchItemTask target = new BenchBatchItemTask(batchDate);

		BomNode root;
		{
			ItemManufacturingMaster entity = new ItemManufacturingMaster();
			entity.setImIId(1);
			root = target.new BomNode(entity);
		}
		{
			ItemConstructionMaster entity = new ItemConstructionMaster();
			entity.setIcIId(2);
			entity.setIcMaterialUnit("g");
			entity.setIcMaterialQuantity(BigDecimal.ONE);
			BomNode node = target.new BomNode(entity);
			root.addChild(node);
		}
		{
			ItemConstructionMaster entity = new ItemConstructionMaster();
			entity.setIcIId(2);
			entity.setIcMaterialUnit("cg");
			entity.setIcMaterialQuantity(BigDecimal.ONE);
			BomNode node = target.new BomNode(entity);
			root.addChild(node);
		}

		target.calculateWeight(root);

		assertEquals(BigDecimal.ZERO, root.weight.value);
		assertEquals("mg", root.weight.unit);
		assertEquals(BigDecimal.valueOf(1000 + 10).setScale(BenchConst.DECIMAL_SCALE), root.weightTotal.value);
		assertEquals("mg", root.weightTotal.unit);
	}
}
