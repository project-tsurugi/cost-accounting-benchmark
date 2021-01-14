package com.example.nedo.online.task;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.InitialData;
import com.example.nedo.init.InitialData03ItemMaster;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

/**
 * 原材料の変更
 */
public class BenchOnlineUpdateMaterialTask extends BenchOnlineTask {

	public BenchOnlineUpdateMaterialTask() {
		super("update-material");
	}

	@Override
	public void execute() {
		logStart("factory=%d, date=%s", factoryId, date);

		tm.required(() -> {
			int select = random.random(0, 1);
			if (select == 0) {
				executeAdd();
			} else {
				executeRemove();
			}
		});

		logEnd("factory=%d, date=%s", factoryId, date);
	}

	protected void executeAdd() {
		// 変更する構成品目を決定
		ItemConstructionMaster item = selectRandomAddItem();

		// 品目構成マスターをロック
		itemCostructionMasterDao.lock(item);

		// 追加する原材料の決定
		ItemMaster material;
		{
			List<ItemMaster> materialList = itemMasterDao.selectByType(date, ItemType.RAW_MATERIAL);
			List<ItemConstructionMaster> childList = itemCostructionMasterDao.selectByParentId(item.getIcIId(), date);
			for (;;) {
				material = selectRandomMaterial(materialList);
				int materialId = material.getIId();
				if (childList.stream().anyMatch(child -> child.getIcIId().intValue() == materialId)) {
					continue;
				}
				break;
			}
		}

		logTarget("add item=%d, parent=%d", material.getIId(), item.getIcIId());

		// 品目構成マスターへ追加
		ItemConstructionMaster entity = newItemConstructionMaster(item, material);
		itemCostructionMasterDao.insert(entity);
	}

	private ItemConstructionMaster selectRandomAddItem() {
		List<ItemType> typeList = Arrays.stream(ItemType.values()).filter(t -> t != ItemType.RAW_MATERIAL)
				.collect(Collectors.toList());
		List<ItemConstructionMaster> list = itemCostructionMasterDao.selectByItemType(date, typeList);
		int i = random.nextInt(list.size());
		return list.get(i);
	}

	private ItemMaster selectRandomMaterial(List<ItemMaster> list) {
		int i = random.nextInt(list.size());
		return list.get(i);
	}

	private static final BigDecimal MQ_START = new BigDecimal("0.1");
	private static final BigDecimal MQ_END = new BigDecimal("10.0");

	private ItemConstructionMaster newItemConstructionMaster(ItemConstructionMaster item, ItemMaster material) {
		ItemConstructionMaster entity = new ItemConstructionMaster();

		entity.setIcIId(material.getIId());
		entity.setIcParentIId(item.getIcIId());
		entity.setIcEffectiveDate(item.getIcEffectiveDate());
		entity.setIcExpiredDate(item.getIcExpiredDate());
		entity.setIcMaterialUnit(material.getIUnit());
		entity.setIcMaterialQuantity(random.random(MQ_START, MQ_END));
		new InitialData03ItemMaster(date).initializeLossRatio(entity.getIcIId() + entity.getIcParentIId(), entity);

		return entity;
	}

	protected void executeRemove() {
		// 変更する構成品目を決定
		ItemConstructionMaster item = selectRandomRemoveItem();

		logTarget("delete item=%d, parent=%d", item.getIcIId(), item.getIcParentIId());

		// 品目構成マスターから削除
		itemCostructionMasterDao.delete(item);
	}

	private ItemConstructionMaster selectRandomRemoveItem() {
		List<ItemType> typeList = Arrays.asList(ItemType.RAW_MATERIAL);
		List<ItemConstructionMaster> list = itemCostructionMasterDao.selectByItemType(date, typeList);
		int i = random.nextInt(list.size());
		return list.get(i);
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineUpdateMaterialTask task = new BenchOnlineUpdateMaterialTask();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		task.setDao(tm, new ItemManufacturingMasterDaoImpl(), new FactoryMasterDaoImpl(), new ItemMasterDaoImpl(),
				new ItemConstructionMasterDaoImpl(), null, new ResultTableDaoImpl());

		task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

		task.execute();
	}
}
