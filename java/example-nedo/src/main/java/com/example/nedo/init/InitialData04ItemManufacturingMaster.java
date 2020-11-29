package com.example.nedo.init;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

public class InitialData04ItemManufacturingMaster extends InitialData {

	public static void main(String[] args) throws Exception {
		LocalDate batchDate = DEFAULT_BATCH_DATE;
		new InitialData04ItemManufacturingMaster(batchDate).main();
	}

	private final Set<Integer> factoryIdSet = new HashSet<>();
	private final Set<Integer> productIdSet = new HashSet<>();

	public InitialData04ItemManufacturingMaster(LocalDate batchDate) {
		super(batchDate);
	}

	private void main() throws IOException {
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
		Map<Integer, Set<Integer>> mapA = generateA();
		Map<Integer, Set<Integer>> map = generateB(mapA);

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

	private Map<Integer, Set<Integer>> generateA() {
		Map<Integer, Set<Integer>> mapA = new HashMap<>(factoryIdSet.size());
		for (int factoryId : factoryIdSet) {
			mapA.put(factoryId, new HashSet<>());
		}

		List<Integer> productIds = new ArrayList<>(this.productIdSet);

		// A all
		for (int i = 0; i < 10; i++) {
			int productId = getRandomAndRemove(productIds);
			for (Set<Integer> list : mapA.values()) {
				list.add(productId);
			}
		}

		// A 50%
		for (int i = 0; i < 20; i++) {
			int productId = getRandomAndRemove(productIds);
			mapA.forEach((factoryId, list) -> {
				if (factoryId.intValue() % 2 == 1) {
					list.add(productId);
				}
			});
		}

		// A 25%
		for (int i = 0; i < 30; i++) {
			int productId = getRandomAndRemove(productIds);
			mapA.forEach((factoryId, list) -> {
				if (factoryId.intValue() % 4 == 1) {
					list.add(productId);
				}
			});
		}

		// A 10%
		for (int i = 0; i < 40; i++) {
			int productId = getRandomAndRemove(productIds);
			mapA.forEach((factoryId, list) -> {
				if (factoryId.intValue() % 10 == 1) {
					list.add(productId);
				}
			});
		}

		return mapA;
	}

	private Map<Integer, Set<Integer>> generateB(Map<Integer, Set<Integer>> map) {
		int factorySize = map.size();
		BigDecimal[] rs = random.split(BigDecimal.valueOf(300 * factorySize), factorySize);

		int i = 0;
		for (Set<Integer> list : map.values()) {
			int breadSize = rs[i++].intValue();

			List<Integer> breadIds = new ArrayList<>(this.productIdSet);
			for (int j = 0; j < breadSize; j++) {
				int breadId;
				for (;;) {
					breadId = getRandomAndRemove(breadIds);
					if (!list.contains(breadId)) {
						break;
					}
				}

				list.add(breadId);
			}
		}

		return map;
	}

	public static void initializeItemManufacturingMasterRandom(BenchRandom random, ItemManufacturingMaster entity) {
		int quantity = random.random(1, 500) * 100;
		entity.setImManufacturingQuantity(BigInteger.valueOf(quantity));
	}
}
