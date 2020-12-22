package com.example.nedo.init;

import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.BenchConst;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

public class InitialData04ItemManufacturingMaster extends InitialData {

	public static void main(String[] args) throws Exception {
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

		initializeField();
		generateItemManufacturingMaster();

		logEnd();
	}

	private void initializeField() {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		{
			FactoryMasterDao dao = new FactoryMasterDaoImpl();
			tm.required(() -> {
				List<Integer> list = dao.selectAllId();

				factoryIdSet.clear();
				factoryIdSet.addAll(list);
				Collections.sort(factoryIdSet);
			});
		}
		{
			ItemMasterDao dao = new ItemMasterDaoImpl();
			tm.required(() -> {
				List<Integer> list = dao.selectIdByType(batchDate, ItemType.PRODUCT);

				productIdSet.clear();
				productIdSet.addAll(list);
			});
		}
	}

	private void generateItemManufacturingMaster() {
		ItemManufacturingMasterDao dao = new ItemManufacturingMasterDaoImpl();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		tm.required(() -> {
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
				dao.insert(entity);
			}
		});
	}

	public ItemManufacturingMaster newItemManufacturingMaster(int factoryId, int productId) {
		ItemManufacturingMaster entity = new ItemManufacturingMaster();

		entity.setImFId(factoryId);
		entity.setImIId(productId);
		initializeStartEndDate(entity);
		initializeItemManufacturingMasterRandom(random, entity);

		return entity;
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

	public static void initializeItemManufacturingMasterRandom(BenchRandom random, ItemManufacturingMaster entity) {
		int quantity = random.random(1, 500) * 100;
		entity.setImManufacturingQuantity(BigInteger.valueOf(quantity));
	}
}
