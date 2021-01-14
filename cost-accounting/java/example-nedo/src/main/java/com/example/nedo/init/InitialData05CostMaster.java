package com.example.nedo.init;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.CostMasterDao;
import com.example.nedo.jdbc.doma2.dao.CostMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.CostMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

public class InitialData05CostMaster extends InitialData {

	public static void main(String[] args) throws Exception {
		LocalDate batchDate = DEFAULT_BATCH_DATE;
		new InitialData05CostMaster(batchDate).main();
	}

	private final Set<Integer> factoryIdSet = new TreeSet<>();

	public InitialData05CostMaster(LocalDate batchDate) {
		super(batchDate);
	}

	private void main() {
		logStart();

		initializeField();
		generateCostMaster();

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
	}

	private void generateCostMaster() {
		CostMasterDao dao = new CostMasterDaoImpl();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();
		tm.required(() -> {
			dao.deleteAll();
		});

		InitialData03ItemMaster itemMasterData = InitialData03ItemMaster.getDefaultInstance();
		int materialStartId = itemMasterData.getMaterialStartId();
		int materialEndId = itemMasterData.getMaterialEndId();
		new CostMasterTask(materialStartId, materialEndId, new ItemMasterDaoImpl(), dao).fork().join();
	}

	@SuppressWarnings("serial")
	private class CostMasterTask extends DaoSplitTask {
		private final ItemMasterDao itemDao;
		private final CostMasterDao dao;

		public CostMasterTask(int startId, int endId, ItemMasterDao itemDao, CostMasterDao dao) {
			super(startId, endId);
			this.itemDao = itemDao;
			this.dao = dao;
		}

		@Override
		protected DaoSplitTask createTask(int startId, int endId) {
			return new CostMasterTask(startId, endId, itemDao, dao);
		}

		@Override
		protected void execute(int iId) {
			insertCostMaster(iId, itemDao, dao);
		}
	}

	private static final BigDecimal Q_START = new BigDecimal("1.0");
	private static final BigDecimal Q_END = new BigDecimal("10.0");
	private static final BigDecimal Q_MULT = new BigDecimal("100");
	private static final BigDecimal A_START = new BigDecimal("0.90");
	private static final BigDecimal A_END = new BigDecimal("1.10");
	private static final BigDecimal A_MIN = new BigDecimal("0.01");

	private void insertCostMaster(int materialId, ItemMasterDao itemDao, CostMasterDao dao) {
		ItemMaster materialEntity = itemDao.selectById(materialId, batchDate);
		if (materialEntity.getIType() != ItemType.RAW_MATERIAL) {
			throw new AssertionError(materialEntity);
		}

		int seed = materialEntity.getIId();

		List<Integer> factoryIds = new ArrayList<>(factoryIdSet);
		int size = factoryIdSet.size() / 2; // 50%
		Set<Integer> factorySet = new HashSet<>();
		while (factorySet.size() < size) {
			int factoryId = getRandomAndRemove(seed++, factoryIds);
			factorySet.add(factoryId);
		}

		for (Integer factoryId : factorySet) {
			CostMaster entity = new CostMaster();
			entity.setCFId(factoryId);
			entity.setCIId(materialEntity.getIId());

			switch (MeasurementUtil.toDefaultUnit(materialEntity.getIUnit())) {
			case "g":
				entity.setCStockUnit("kg");
				break;
			case "mL":
				entity.setCStockUnit("L");
				break;
			case "count":
				entity.setCStockUnit("count");
				break;
			default:
				throw new RuntimeException(materialEntity.getIUnit());
			}

			entity.setCStockQuantity(random(seed++, Q_START, Q_END).multiply(Q_MULT));

			BigDecimal price = materialEntity.getIPrice();
			BigDecimal stock = MeasurementUtil.convertUnit(entity.getCStockQuantity(), entity.getCStockUnit(),
					materialEntity.getIPriceUnit());
			BigDecimal amount = price.multiply(random(seed++, A_START, A_END).multiply(stock));
			if (amount.compareTo(A_MIN) < 0) {
				amount = A_MIN;
			}
			entity.setCStockAmount(amount);

			dao.insert(entity);
		}
	}
}
