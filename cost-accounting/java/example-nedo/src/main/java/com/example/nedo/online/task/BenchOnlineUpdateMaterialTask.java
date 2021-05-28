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
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMasterKey;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

/**
 * 原材料の変更
 */
public class BenchOnlineUpdateMaterialTask extends BenchOnlineTask {

	public BenchOnlineUpdateMaterialTask() {
		super("update-material");
	}

	@Override
	protected boolean execute1() {
		return dbManager.execute(() -> {
			int select = random.random(0, 1);
			if (select == 0) {
				return executeAdd();
			} else {
				return executeRemove();
			}
		});
	}

	protected boolean executeAdd() {
		// 変更する構成品目を決定
		ItemConstructionMaster item = selectRandomAddItem();

		// 品目構成マスターをロック
		ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
		itemCostructionMasterDao.lock(item);

		// 追加する原材料の決定
		ItemMaster material;
		{
			List<Integer> materialList = selectMaterial();
			List<ItemConstructionMaster> childList = itemCostructionMasterDao.selectByParentId(item.getIcIId(), date);
			int materialId = -1;
			for (int j = 0; j < materialList.size(); j++) {
				int mid;
				{
					int i = random.nextInt(materialList.size());
					mid = materialList.get(i);
				}
				if (childList.stream().anyMatch(child -> child.getIcIId().intValue() == mid)) {
					continue;
				}
				materialId = mid;
				break;
			}
			if (materialId < 0) {
				return false; // retry over
			}
			ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
			material = itemMasterDao.selectById(materialId, date);
		}

		logTarget("add item=%d, parent=%d", material.getIId(), item.getIcIId());

		// 品目構成マスターへ追加
		ItemConstructionMaster entity = newItemConstructionMaster(item, material);
		itemCostructionMasterDao.insert(entity);

		return true;
	}

	private static List<ItemConstructionMasterKey> itemConstructionMasterKeyListForAdd;

	private ItemConstructionMaster selectRandomAddItem() {
		ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();

		ItemConstructionMaster entity;
		for (;;) {
			ItemConstructionMasterKey key;
			synchronized (BenchOnlineUpdateMaterialTask.class) {
				if (itemConstructionMasterKeyListForAdd == null) {
					List<ItemType> typeList = Arrays.stream(ItemType.values()).filter(t -> t != ItemType.RAW_MATERIAL)
							.collect(Collectors.toList());
					itemConstructionMasterKeyListForAdd = itemCostructionMasterDao.selectByItemType(date, typeList);
				}
				List<ItemConstructionMasterKey> list = itemConstructionMasterKeyListForAdd;
				int i = random.nextInt(list.size());
				key = list.get(i);
			}
			entity = itemCostructionMasterDao.selectById(key.getIcParentIId(), key.getIcIId(),
					key.getIcEffectiveDate());
			if (entity != null) {
				break;
			}
		}
		return entity;
	}

	private static List<Integer> itemMasterMaterialKeyList;

	private List<Integer> selectMaterial() {
		synchronized (BenchOnlineUpdateMaterialTask.class) {
			if (itemMasterMaterialKeyList == null) {
				ItemMasterDao itemMasterDao = dbManager.getItemMasterDao();
				itemMasterMaterialKeyList = itemMasterDao.selectIdByType(date, ItemType.RAW_MATERIAL);
			}
		}
		return itemMasterMaterialKeyList;
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

	protected boolean executeRemove() {
		// 変更する構成品目を決定
		ItemConstructionMasterKey item = selectRandomRemoveItem();
		if (item == null) {
			return false;
		}

		logTarget("delete item=%d, parent=%d", item.getIcIId(), item.getIcParentIId());

		// 品目構成マスターから削除
		ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
		itemCostructionMasterDao.delete(item);

		return true;
	}

	private static List<ItemConstructionMasterKey> itemConstructionMasterKeyListForRemove = null;

	private ItemConstructionMasterKey selectRandomRemoveItem() {
		ItemConstructionMasterKey key;
		synchronized (BenchOnlineUpdateMaterialTask.class) {
			if (itemConstructionMasterKeyListForRemove == null) {
				List<ItemType> typeList = Arrays.asList(ItemType.RAW_MATERIAL);
				ItemConstructionMasterDao itemCostructionMasterDao = dbManager.getItemConstructionMasterDao();
				itemConstructionMasterKeyListForRemove = itemCostructionMasterDao.selectByItemType(date, typeList);
			}
			List<ItemConstructionMasterKey> list = itemConstructionMasterKeyListForRemove;
			if (list.isEmpty()) {
				return null;
			}
			int i = random.nextInt(list.size());
			key = list.remove(i);
		}
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
