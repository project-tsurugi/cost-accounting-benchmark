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
import java.util.concurrent.ForkJoinTask;
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
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;

@SuppressWarnings("serial")
public class InitialData03ItemMaster extends InitialData {

	public static void main(String[] args) throws Exception {
		InitialData03ItemMaster instance = getDefaultInstance();
		instance.main();
	}

	public static InitialData03ItemMaster getDefaultInstance() {
		int productSize = BenchConst.initItemProductSize();
		int workSize = BenchConst.initItemWorkSize();
		int materialSize = BenchConst.initItemMaterialSize();

		LocalDate batchDate = DEFAULT_BATCH_DATE;
		return new InitialData03ItemMaster(productSize, workSize, materialSize, batchDate);
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

	public int getProductEndId() {
		return getProductStartId() + productSize;
	}

	public int getWorkStartId() {
		return getMaterialEndId();
	}

	public int getWorkEndId() {
		return getWorkStartId() + workSize;
	}

	public int getMaterialStartId() {
		return getProductEndId();
	}

	public int getMaterialEndId() {
		return getMaterialStartId() + materialSize;
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
		});

		ForkJoinTask<Void> ptask = new ItemMasterProductTask(getProductStartId(), getProductEndId(), dao).fork();
		ForkJoinTask<Void> mtask = new ItemMasterMaterialTask(getMaterialStartId(), getMaterialEndId(), dao).fork();
		ptask.join();
		mtask.join();
		forkItemMasterWorkInProcess(getWorkStartId(), getWorkEndId(), dao, icDao);
		new ItemConstructionMasterProductTask(getProductEndId(), getProductEndId(), icDao).fork().join();
	}

	private abstract class ItemMasterTask extends DaoSplitTask {
		protected final ItemMasterDao dao;

		public ItemMasterTask(int startId, int endId, ItemMasterDao dao) {
			super(startId, endId);
			this.dao = dao;
		}
	}

	private class ItemMasterProductTask extends ItemMasterTask {
		public ItemMasterProductTask(int startId, int endId, ItemMasterDao dao) {
			super(startId, endId, dao);
		}

		@Override
		protected ItemMasterTask createTask(int startId, int endId) {
			return new ItemMasterProductTask(startId, endId, dao);
		}

		@Override
		protected void execute(int iId) {
			ItemMaster entity = newItemMasterProduct(iId);
			insertItemMaster(dao, entity, null);
		}
	}

	private class ItemMasterMaterialTask extends ItemMasterTask {
		public ItemMasterMaterialTask(int startId, int endId, ItemMasterDao dao) {
			super(startId, endId, dao);
		}

		@Override
		protected ItemMasterTask createTask(int startId, int endId) {
			return new ItemMasterMaterialTask(startId, endId, dao);
		}

		@Override
		protected void execute(int iId) {
			ItemMaster entity = newItemMasterMaterial(iId);
			insertItemMaster(dao, entity, InitialData03ItemMaster.this::randomtItemMasterMaterial);
		}
	}

	public ItemMaster newItemMaster(int iId) {
		ItemType type;
		if (getProductStartId() <= iId && iId < getProductStartId() + productSize) {
			type = ItemType.PRODUCT;
		} else if (getWorkStartId() <= iId && iId < getWorkStartId() + workSize) {
			type = ItemType.WORK_IN_PROCESS;
		} else if (getMaterialStartId() <= iId && iId < getMaterialStartId() + materialSize) {
			type = ItemType.RAW_MATERIAL;
		} else {
			throw new IllegalArgumentException("iId=" + iId);
		}

		switch (type) {
		case PRODUCT:
			return newItemMasterProduct(iId);
		case WORK_IN_PROCESS:
			return newItemMasterWork(iId);
		case RAW_MATERIAL:
			return newItemMasterMaterial(iId);
		default:
			throw new InternalError("type=" + type);
		}
	}

	public ItemMaster newItemMasterProduct(int iId) {
		// iIdが同じであれば、同じ内容を返すようにする
		ItemMaster entity = new ItemMaster();

		entity.setIId(iId);
		initializeStartEndDate(iId, entity);
		entity.setIName("Bread" + iId);
		entity.setIType(ItemType.PRODUCT);
		entity.setIUnit("count");

		return entity;
	}

	private ItemMaster newItemMasterWork(int iId) {
		// iIdが同じであれば、同じ内容を返すようにする
		ItemMaster entity = new ItemMaster();

		entity.setIId(iId);
		initializeStartEndDate(iId, entity);
		entity.setIName("Process" + entity.getIId());
		entity.setIType(ItemType.WORK_IN_PROCESS);

		return entity;
	}

	private ItemMaster newItemMasterMaterial(int iId) {
		// iIdが同じであれば、同じ内容を返すようにする
		ItemMaster entity = new ItemMaster();

		entity.setIId(iId);
		initializeStartEndDate(iId, entity);
		entity.setIName("Material" + iId);
		entity.setIType(ItemType.RAW_MATERIAL);
		randomtItemMasterMaterial(entity);

		return entity;
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
		int seed = entity.getIId();
		String unit = MATERIAL_UNIT[random.prandom(seed, MATERIAL_UNIT.length)];
		entity.setIUnit(unit);

		switch (unit) {
		case "mL":
		case "cL":
		case "dL":
			entity.setIWeightRatio(BigDecimal.ONE.add(random(++seed, W_START, W_END)));
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
			entity.setIPrice(random(++seed, PRICE_START, PRICE_END));
			entity.setIPriceUnit("L");
			break;
		case "mg":
		case "cg":
		case "dg":
		case "g":
			entity.setIWeightRatio(BigDecimal.ONE);
			entity.setIWeightUnit(unit);
			entity.setIPrice(random(++seed, PRICE_START, PRICE_END));
			entity.setIPriceUnit("kg");
			break;
		default:
			entity.setIWeightRatio(random(++seed, C_START, C_END));
			entity.setIWeightUnit("g");
			entity.setIPrice(random(++seed, PRICE_START, PRICE_END)
					.divide(SCALE3, BenchConst.DECIMAL_SCALE, RoundingMode.DOWN).multiply(entity.getIWeightRatio()));
			entity.setIPriceUnit(unit);
			break;
		}
	}

	private static final int TASK_THRESHOLD = DaoSplitTask.TASK_THRESHOLD;

	private void forkItemMasterWorkInProcess(int startId, int endId, ItemMasterDao dao,
			ItemConstructionMasterDao icDao) {
		List<ItemMasterWorkTask> taskList = new ArrayList<>();
		ItemMasterWorkTask task = new ItemMasterWorkTask(dao, icDao);

		int id = startId;
		int count = 0;
		final int size = endId - startId;
		while (count < size) {
			// 木の要素数を決定する
			int treeSize = 10 + random(id, -4, 4);
			if (count + treeSize > size) {
				treeSize = size - count;
			}
			count += treeSize;

			task.add(id, id + treeSize);
			if (task.idSize() >= TASK_THRESHOLD) {
				task.fork();
				taskList.add(task);
				task = new ItemMasterWorkTask(dao, icDao);
			}

			id += treeSize;
		}

		task.fork();
		taskList.add(task);

		for (ItemMasterWorkTask t : taskList) {
			t.join();
		}
	}

	private static class Range {
		public final int startId;
		public final int endId;

		public Range(int startId, int endId) {
			this.startId = startId;
			this.endId = endId;
		}
	}

	private class ItemMasterWorkTask extends DaoListTask<Range> {
		private final ItemMasterDao dao;
		private final ItemConstructionMasterDao icDao;

		private int idSize = 0;

		public ItemMasterWorkTask(ItemMasterDao dao, ItemConstructionMasterDao icDao) {
			this.dao = dao;
			this.icDao = icDao;
		}

		public void add(int startId, int endId) {
			super.add(new Range(startId, endId));
			int size = endId - startId;
			this.idSize += size;
		}

		public int idSize() {
			return idSize;
		}

		@Override
		protected void execute(Range range) {
			insertItemMasterWorkInProcess(range.startId, range.endId, dao, icDao);
		}
	}

	private class Node {
		int itemId;
		ItemMaster entity;

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
			this.itemId = iId.getAndIncrement();

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

	private void insertItemMasterWorkInProcess(int startId, int endId, ItemMasterDao dao,
			ItemConstructionMasterDao icDao) {
		AtomicInteger iId = new AtomicInteger(startId);

		final int treeSize = endId - startId;
		int seed = startId;

		// ルート要素を作成する
		Node root = new Node(null);
		List<Node> nodeList = new ArrayList<>();
		nodeList.add(root);

		// 要素を追加していく
		while (nodeList.size() < treeSize) {
			Node node = new Node(null);

			Node parent = nodeList.get(random.prandom(++seed, nodeList.size()));
			parent.addChild(node);

			nodeList.add(node);
		}

		// 品目IDを割り当てる
		root.assignId(iId);
		for (Node node : nodeList) {
			node.entity = newItemMasterWork(node.itemId);
			dao.insert(node.entity);
		}

		// 材料を割り当てる
		for (Node node : nodeList) {
			if (!node.isLeaf()) {
				continue;
			}

			int materialSize = random(++seed, 1, 3);
			List<ItemMaster> materialList = findRandomMaterial(++seed, materialSize, dao);
			for (ItemMaster material : materialList) {
				node.addChild(new Node(material));
			}
		}

		// 重量を割り当てる
		{
			BigDecimal weight = random(++seed, WEIGHT_START, WEIGHT_END);
			root.setWeight(weight);
		}

		// 品目構成マスターの生成
		for (Node node : root.toNodeList()) {
			if (node.parent == null) {
				continue;
			}
			ItemMaster item = node.entity;

			ItemConstructionMaster entity = new ItemConstructionMaster();
			entity.setIcIId(item.getIId());
			entity.setIcParentIId(node.parent.entity.getIId());
			entity.setIcEffectiveDate(item.getEffectiveDate());
			entity.setIcExpiredDate(item.getExpiredDate());

			// material
			if (item.getIType() == ItemType.RAW_MATERIAL) {
				entity.setIcMaterialUnit(item.getIUnit());

				BigDecimal weight = MeasurementUtil.convertUnit(node.weight, "g", item.getIWeightUnit());
				entity.setIcMaterialQuantity(
						weight.divide(item.getIWeightRatio(), BenchConst.DECIMAL_SCALE, RoundingMode.HALF_UP));
			}

			initializeLossRatio(entity.getIcIId() + entity.getIcParentIId(), entity);

			icDao.insert(entity);
		}
	}

	// 製品品目の品目構成マスター
	private class ItemConstructionMasterProductTask extends DaoSplitTask {
		protected final ItemConstructionMasterDao icDao;

		public ItemConstructionMasterProductTask(int startId, int endId, ItemConstructionMasterDao icDao) {
			super(startId, endId);
			this.icDao = icDao;
		}

		@Override
		protected ItemConstructionMasterProductTask createTask(int startId, int endId) {
			return new ItemConstructionMasterProductTask(startId, endId, icDao);
		}

		@Override
		protected void execute(int iId) {
			final int workStart = getWorkStartId();
			final int workEnd = getWorkEndId() - 1;

			int seed = iId;
			int s = random(seed, 1, PRODUCT_TREE_SIZE);
			Set<Integer> set = new HashSet<>(s);
			while (set.size() < s) {
				set.add(random(++seed, workStart, workEnd));
			}

			insertItemConstructionMasterProduct(iId, set, icDao);
		}
	}

	private static final BigDecimal LOSS_END = new BigDecimal("10.00");

	public void initializeLossRatio(int seed, ItemConstructionMaster entity) {
		entity.setIcLossRatio(random.random0(seed, LOSS_END));
	}

	public static final int PRODUCT_TREE_SIZE = 5;

	public void insertItemConstructionMasterProduct(int productId, Set<Integer> workSet,
			ItemConstructionMasterDao icDao) {
		for (Integer workId : workSet) {
			ItemConstructionMaster entity = new ItemConstructionMaster();
			entity.setIcIId(workId);
			entity.setIcParentIId(productId);
			initializeStartEndDate(workId + productId, entity);
			initializeItemConstructionMasterRandom(random, entity);

			icDao.insert(entity);
		}
	}

	private List<ItemMaster> findRandomMaterial(int seed, int size, ItemMasterDao dao) {
		int materialStartId = getMaterialStartId();
		int materialEndId = materialStartId + materialSize - 1;

		Set<Integer> idSet = new TreeSet<>();
		while (idSet.size() < size) {
			idSet.add(random(seed++, materialStartId, materialEndId));
		}

		return dao.selectByIds(idSet, batchDate);
	}

	private void insertItemMaster(ItemMasterDao dao, ItemMaster entity, Consumer<ItemMaster> initializer) {
		TreeMap<LocalDate, ItemMaster> map = new TreeMap<>();
		map.put(entity.getIEffectiveDate(), entity);

		for (int i = 0; i < 2; i++) { // 3倍に増幅する
			ItemMaster ent;
			int seed = entity.getIId() + i;
			if (random(seed, 0, 1) == 0) {
				ItemMaster src = map.firstEntry().getValue();
				ent = src.clone();
				initializePrevStartEndDate(seed + 1, src, ent);
			} else {
				ItemMaster src = map.lastEntry().getValue();
				ent = src.clone();
				initializeNextStartEndDate(seed + 1, src, ent);
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
		int seed = entity.getIcIId();
		entity.setIcLossRatio(random.random0(seed, LOSS_END));
	}
}
