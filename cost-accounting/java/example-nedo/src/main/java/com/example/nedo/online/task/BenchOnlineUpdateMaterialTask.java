package com.example.nedo.online.task;

import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import com.example.nedo.init.InitialData;
import com.example.nedo.init.InitialData03ItemMaster;
import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
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
	protected void execute1() {
		dbManager.execute(() -> {
			int select = random.random(0, 1);
			if (select == 0) {
				executeAdd();
			} else {
				executeRemove();
			}
		});
	}

	protected void executeAdd() {
		// 変更する構成品目を決定
		ItemConstructionMaster item = selectRandomAddItem();

		// 品目構成マスターをロック
		ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
		itemCostructionMasterDao.lock(item);

		// 追加する原材料の決定
		ItemMaster material;
		{
			ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
			List<Integer> materialList = itemMasterDao.selectIdByType(date, ItemType.RAW_MATERIAL);
			List<ItemConstructionMaster> childList = itemCostructionMasterDao.selectByParentId(item.getIcIId(), date);
			int materialId;
			for (;;) {
				int mid = selectRandomMaterial(materialList);
				if (childList.stream().anyMatch(child -> child.getIcIId().intValue() == mid)) {
					continue;
				}
				materialId = mid;
				break;
			}
			material = itemMasterDao.selectById(materialId, date);
		}

		logTarget("add item=%d, parent=%d", material.getIId(), item.getIcIId());

		// 品目構成マスターへ追加
		ItemConstructionMaster entity = newItemConstructionMaster(item, material);
		itemCostructionMasterDao.insert(entity);
	}

	private ItemConstructionMaster selectRandomAddItem() {
		List<ItemType> typeList = Arrays.stream(ItemType.values()).filter(t -> t != ItemType.RAW_MATERIAL)
				.collect(Collectors.toList());
		ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
		List<ItemConstructionMaster> list = itemCostructionMasterDao.selectByItemType(date, typeList);
		int i = random.nextInt(list.size());
		ItemConstructionMaster key = list.get(i);
		ItemConstructionMaster entity = itemCostructionMasterDao.selectById(key.getIcParentIId(), key.getIcIId(),
				key.getIcEffectiveDate());
		if (entity == null) {
			throw new IllegalStateException("selectRandomAddItem key=" + key);
		}
		return entity;
	}

	private Integer selectRandomMaterial(List<Integer> list) {
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
		ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
		itemCostructionMasterDao.delete(item);
	}

	private ItemConstructionMaster selectRandomRemoveItem() {
		List<ItemType> typeList = Arrays.asList(ItemType.RAW_MATERIAL);
		ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
		List<ItemConstructionMaster> list = itemCostructionMasterDao.selectByItemType(date, typeList);
		int i = random.nextInt(list.size());
		ItemConstructionMaster key = list.get(i);
		return key;
	}

	// for test
	public static void main(String[] args) {
		BenchOnlineUpdateMaterialTask task = new BenchOnlineUpdateMaterialTask();

		try (CostBenchDbManager manager = createCostBenchDbManagerForTest()) {
			task.setDao(manager);

			task.initialize(1, InitialData.DEFAULT_BATCH_DATE);

			task.execute();
		}
	}
}
