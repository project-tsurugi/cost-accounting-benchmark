package com.example.nedo.init;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.BenchConst;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.domain.ItemType;
import com.example.nedo.jdbc.doma2.entity.HasDateRange;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

public class InitialData03ItemMaster extends InitialData {

	public static void main(String[] args) throws Exception {
		int productSize = BenchConst.initItemProductSize();
		int workSize = BenchConst.initItemWorkSize();
		int materialSize = BenchConst.initItemMaterialSize();

		LocalDate batchDate = DEFAULT_BATCH_DATE;
		new InitialData03ItemMaster(productSize, workSize, materialSize, batchDate).main();
	}

	private final int productSize;
	private final int workSize;
	private final int materialSize;

	public InitialData03ItemMaster(int productSize, int workSize, int materialSize, LocalDate batchDate) {
		super(batchDate);
		this.productSize = productSize;
		this.workSize = workSize;
		this.materialSize = materialSize;
	}

	public InitialData03ItemMaster(LocalDate batchDate) {
		super(batchDate);
		this.productSize = -1;
		this.workSize = -1;
		this.materialSize = -1;
	}

	public int getProductStartId() {
		return 1;
	}

	public int getWorkStartId() {
		return getMaterialStartId() + materialSize;
	}

	public int getMaterialStartId() {
		return getProductStartId() + productSize;
	}

	private void main() {
		logStart();

		generateItemMaster();

		logEnd();
	}

	private void generateItemMaster() {
		ItemMasterDao dao = new ItemMasterDaoImpl();
		ItemConstructionMasterDao icDao = new ItemConstructionMasterDaoImpl();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		tm.required(() -> {
			dao.deleteAll();
			icDao.deleteAll();
			insertItemMasterProduct(getProductStartId(), productSize, dao);
			insertItemMasterMaterial(getMaterialStartId(), materialSize, dao);
			insertItemMasterWorkInProcess(getWorkStartId(), workSize, dao, icDao);
		});
	}

	private void insertItemMasterProduct(int startId, int size, ItemMasterDao dao) {
		for (int i = 0, iId = startId; i < size; i++, iId++) {
			ItemMaster entity = newItemMasterProduct(iId);
			initializeStartEndDate(entity);
			insertItemMaster(dao, entity, null);
		}
	}

	public ItemMaster newItemMasterProduct(int iId) {
		ItemMaster entity = new ItemMaster();

		entity.setIId(iId);
		entity.setIName("Bread" + iId);
		entity.setIType(ItemType.PRODUCT);
		entity.setIUnit("count");

		return entity;
	}

	private void insertItemMasterMaterial(int startId, int size, ItemMasterDao dao) {
		for (int i = 0, iId = startId; i < size; i++, iId++) {
			ItemMaster entity = new ItemMaster();
			entity.setIId(iId);
			initializeStartEndDate(entity);
			entity.setIName("Material" + iId);
			entity.setIType(ItemType.RAW_MATERIAL);

			insertItemMaster(dao, entity, this::randomtItemMasterMaterial);
		}
	}

	private static final String[] MATERIAL_UNIT = { "mL", "cL", "dL", "mg", "cg", "dg", "g", "count" };
	private static final BigDecimal W_START = new BigDecimal("-0.10");
	private static final BigDecimal W_END = new BigDecimal("0.10");
	private static final BigDecimal C_START = new BigDecimal("0.4");
	private static final BigDecimal C_END = new BigDecimal("500.0");
	private static final BigDecimal PRICE_START = new BigDecimal("0.01");
	private static final BigDecimal PRICE_END = new BigDecimal("2000.00");
	private static final BigDecimal SCALE3 = BigDecimal.valueOf(1000);

	private void randomtItemMasterMaterial(ItemMaster entity) {
		String unit = MATERIAL_UNIT[random.nextInt(MATERIAL_UNIT.length)];
		entity.setIUnit(unit);

		switch (unit) {
		case "mL":
		case "cL":
		case "dL":
			entity.setIWeightRatio(BigDecimal.ONE.add(random(W_START, W_END)));
			switch (unit) {
			case "mL":
				entity.setIWeightUnit("g");
				break;
			case "cL":
				entity.setIWeightUnit("dag");
				break;
			case "dL":
				entity.setIWeightUnit("hg");
				break;
			}
			entity.setIPrice(random(PRICE_START, PRICE_END));
			entity.setIPriceUnit("L");
			break;
		case "mg":
		case "cg":
		case "dg":
		case "g":
			entity.setIWeightRatio(BigDecimal.ONE);
			entity.setIWeightUnit(unit);
			entity.setIPrice(random(PRICE_START, PRICE_END));
			entity.setIPriceUnit("kg");
			break;
		default:
			entity.setIWeightRatio(random(C_START, C_END));
			entity.setIWeightUnit("g");
			entity.setIPrice(random(PRICE_START, PRICE_END).divide(SCALE3, BenchConst.DECIMAL_SCALE, RoundingMode.DOWN)
					.multiply(entity.getIWeightRatio()));
			entity.setIPriceUnit(unit);
			break;
		}
	}

	private class Node {
		final ItemMaster entity;

		Node parent = null;
		final List<Node> childList = new ArrayList<>();

		BigDecimal weight;

		public Node(ItemMaster entity) {
			this.entity = entity;
		}

		public void addChild(Node child) {
			child.parent = this;
			childList.add(child);
		}

		public void assignId(AtomicInteger iId) {
			entity.setIId(iId.getAndIncrement());

			for (Node child : childList) {
				child.assignId(iId);
			}
		}

		public boolean isLeaf() {
			return childList.isEmpty();
		}

		public void setWeight(BigDecimal weight) {
			this.weight = weight;

			if (!childList.isEmpty()) {
				BigDecimal[] ws = random.split(weight, childList.size());

				int i = 0;
				for (Node child : childList) {
					child.setWeight(ws[i++]);
				}
			}
		}

		public List<Node> toNodeList() {
			List<Node> list = new ArrayList<>();
			collectNode(list);
			return list;
		}

		private void collectNode(List<Node> list) {
			list.add(this);

			for (Node child : childList) {
				child.collectNode(list);
			}
		}
	}

	private static final BigDecimal WEIGHT_START = new BigDecimal("20.0000");
	private static final BigDecimal WEIGHT_END = new BigDecimal("100.0000");

	private void insertItemMasterWorkInProcess(int startId, int size, ItemMasterDao dao,
			ItemConstructionMasterDao icDao) {
		AtomicInteger iId = new AtomicInteger(startId);

		int count = 0;
		while (count < size) {
			// 木の要素数を決定する
			int seed = startId + count;
			int treeSize = 10 + random.prandom(seed, -4, 4);
			if (count + treeSize > size) {
				treeSize = size - count;
			}
			count += treeSize;

			// ルート要素を作成する
			Node root = new Node(new ItemMaster());
			List<Node> nodeList = new ArrayList<>();
			nodeList.add(root);

			// 要素を追加していく
			while (nodeList.size() < treeSize) {
				Node node = new Node(new ItemMaster());

				Node parent = nodeList.get(random.prandom(++seed, nodeList.size()));
				parent.addChild(node);

				nodeList.add(node);
			}

			// 品目IDを割り当てる
			root.assignId(iId);
			for (Node node : nodeList) {
				ItemMaster entity = node.entity;
				initializeStartEndDate(entity);
				entity.setIName("Process" + entity.getIId());
				entity.setIType(ItemType.WORK_IN_PROCESS);

				dao.insert(entity);
			}

			// 材料を割り当てる
			for (Node node : nodeList) {
				if (!node.isLeaf()) {
					continue;
				}

				int materialSize = random.prandom(++seed, 1, 3);
				List<ItemMaster> materialList = findRandomMaterial(++seed, materialSize, dao);
				for (ItemMaster material : materialList) {
					node.addChild(new Node(material));
				}
			}

			// 重量を割り当てる
			{
				BigDecimal weight = random(WEIGHT_START, WEIGHT_END);
				root.setWeight(weight);
			}

			// 品目構成マスターの生成
			HasDateRange startEndDate = new ItemConstructionMaster();
			initializeStartEndDate(startEndDate);
			for (Node node : root.toNodeList()) {
				if (node.parent == null) {
					continue;
				}
				ItemMaster item = node.entity;

				ItemConstructionMaster entity = new ItemConstructionMaster();
				entity.setIcIId(item.getIId());
				entity.setIcParentIId(node.parent.entity.getIId());
				entity.setIcEffectiveDate(startEndDate.getEffectiveDate());
				entity.setIcExpiredDate(startEndDate.getExpiredDate());

				// material
				if (item.getIType() == ItemType.RAW_MATERIAL) {
					entity.setIcMaterialUnit(item.getIUnit());

					BigDecimal weight = MeasurementUtil.convertUnit(node.weight, "g", item.getIWeightUnit());
					entity.setIcMaterialQuantity(
							weight.divide(item.getIWeightRatio(), BenchConst.DECIMAL_SCALE, RoundingMode.HALF_UP));
				}

				initializeLossRatio(entity);

				icDao.insert(entity);
			}
		}

		// 製品品目の品目構成マスター
		{
			int workStart = startId;
			int workEnd = iId.get() - 1;
			int productId = getProductStartId();
			for (int i = 0; i < productSize; i++, productId++) {
				int seed = productId;
				int s = random.prandom(seed, 1, PRODUCT_TREE_SIZE);
				Set<Integer> set = new HashSet<>(s);
				while (set.size() < s) {
					set.add(random.prandom(++seed, workStart, workEnd));
				}

				insertItemConstructionMasterProduct(productId, set, icDao);
			}
		}
	}

	private static final BigDecimal LOSS_END = new BigDecimal("10.00");

	public void initializeLossRatio(ItemConstructionMaster entity) {
		entity.setIcLossRatio(random.random0(LOSS_END));
	}

	public static final int PRODUCT_TREE_SIZE = 5;

	public void insertItemConstructionMasterProduct(int productId, Set<Integer> workSet,
			ItemConstructionMasterDao icDao) {
		for (Integer workId : workSet) {
			ItemConstructionMaster entity = new ItemConstructionMaster();
			entity.setIcIId(workId);
			entity.setIcParentIId(productId);
			initializeStartEndDate(entity);
			initializeItemConstructionMasterRandom(random, entity);

			icDao.insert(entity);
		}
	}

	private List<ItemMaster> findRandomMaterial(int seed, int size, ItemMasterDao dao) {
		int materialStartId = getMaterialStartId();
		int materialEndId = materialStartId + materialSize - 1;

		Set<Integer> idSet = new TreeSet<>();
		while (idSet.size() < size) {
			idSet.add(random.prandom(seed++, materialStartId, materialEndId));
		}

		return dao.selectByIds(idSet, batchDate);
	}

	private void insertItemMaster(ItemMasterDao dao, ItemMaster entity, Consumer<ItemMaster> initializer) {
		TreeMap<LocalDate, ItemMaster> map = new TreeMap<>();
		map.put(entity.getIEffectiveDate(), entity);

		for (int i = 0; i < 2; i++) { // 3倍に増幅する
			ItemMaster ent;
			int seed = entity.getIId() + i;
			if (random.prandom(seed, 0, 1) == 0) {
				ItemMaster src = map.firstEntry().getValue();
				ent = src.clone();
				initializePrevStartEndDate(src, ent);
			} else {
				ItemMaster src = map.lastEntry().getValue();
				ent = src.clone();
				initializeNextStartEndDate(src, ent);
			}

			map.put(ent.getIEffectiveDate(), ent);
		}

		map.values().forEach(ent -> {
			if (initializer != null) {
				initializer.accept(ent);
			}
			dao.insert(ent);
		});
	}

	public static void initializeItemConstructionMasterRandom(BenchRandom random, ItemConstructionMaster entity) {
		entity.setIcLossRatio(random.random0(LOSS_END));
	}
}
