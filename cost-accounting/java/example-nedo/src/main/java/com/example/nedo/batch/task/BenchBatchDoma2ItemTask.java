package com.example.nedo.batch.task;

import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.stream.Stream;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.InitialData;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.CostMasterDao;
import com.example.nedo.jdbc.doma2.dao.CostMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.jdbc.doma2.entity.CostMaster;
import com.example.nedo.jdbc.doma2.entity.ItemConstructionMaster;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;
import com.example.nedo.jdbc.doma2.entity.ItemMaster;
import com.example.nedo.jdbc.doma2.entity.ResultTable;

public class BenchBatchDoma2ItemTask extends BenchBatchItemTask {

	private ItemConstructionMasterDao ItemConstructionMasterDao;
	private ItemMasterDao itemMasterDao;
	private CostMasterDao costMasterDao;
	private ResultTableDao resultTableDao;

	public BenchBatchDoma2ItemTask(LocalDate batchDate) {
		super(batchDate);
	}

	public void setDao(ItemConstructionMasterDao ItemConstructionMasterDao, ItemMasterDao itemMasterDao,
			CostMasterDao costMasterDao, ResultTableDao resultTableDao) {
		this.ItemConstructionMasterDao = ItemConstructionMasterDao;
		this.itemMasterDao = itemMasterDao;
		this.costMasterDao = costMasterDao;
		this.resultTableDao = resultTableDao;
	}

	@Override
	protected List<ItemConstructionMaster> selectItemConstructionMaster(int parentItemId, LocalDate batchDate) {
		return ItemConstructionMasterDao.selectByParentId(parentItemId, batchDate);
	}

	@Override
	protected Stream<ItemConstructionMaster> selectItemConstructionMasterRecursive(int parentItemId,
			LocalDate batchDate) {
		return ItemConstructionMasterDao.selectRecursiveByParentId(parentItemId, batchDate);
	}

	@Override
	protected ItemMaster selectItemMaster(int itemId) {
		return itemMasterDao.selectById(itemId, batchDate);
	}

	@Override
	protected CostMaster selectCostMaster(int factoryId, int itemId) {
		return costMasterDao.selectById(factoryId, itemId);
	}

	@Override
	protected void insertResultTable(ResultTable entity) {
		resultTableDao.insert(entity);
	}

	@Override
	protected void insertResultTable(Collection<ResultTable> list) {
		resultTableDao.insertBatch(list);
	}

	// for test
	public static void main(String[] args) {
//		test1();
		test2();
	}

	static void test1() {
		BenchBatchDoma2ItemTask task = new BenchBatchDoma2ItemTask(InitialData.DEFAULT_BATCH_DATE);
		task.setDao(new ItemConstructionMasterDaoImpl(), new ItemMasterDaoImpl(), new CostMasterDaoImpl(),
				new ResultTableDaoImpl());

		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		tm.required(() -> {
			ItemManufacturingMasterDao itemManufacturingMasterDao = new ItemManufacturingMasterDaoImpl();
			ItemManufacturingMaster manufact = itemManufacturingMasterDao.selectById(1, 67586,
					InitialData.DEFAULT_BATCH_DATE);
			BomNode root1 = task.new BomNode(manufact);
			{
				long s = System.currentTimeMillis();
				task.createBomTree1(root1);
				long e = System.currentTimeMillis();
				System.out.printf("1: %d\n", e - s);
			}
			BomNode root2 = task.new BomNode(manufact);
			{
				long s = System.currentTimeMillis();
				task.createBomTree2(root2);
				long e = System.currentTimeMillis();
				System.out.printf("2: %d\n", e - s);
			}

			System.out.println(root1.equalsById(root2));
		});
	}

	static void test2() {
		int itemId = 11649;
		LocalDate date = InitialData.DEFAULT_BATCH_DATE;
//		LocalDate date = LocalDate.of(2020, 9, 23);

		BenchBatchDoma2ItemTask task = new BenchBatchDoma2ItemTask(date);
		task.setDao(new ItemConstructionMasterDaoImpl(), new ItemMasterDaoImpl(), new CostMasterDaoImpl(),
				new ResultTableDaoImpl());

		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		tm.required(() -> {
			ResultTableDao resultTableDao = new ResultTableDaoImpl();
			resultTableDao.deleteByProductId(1, date, itemId);

			ItemManufacturingMasterDao itemManufacturingMasterDao = new ItemManufacturingMasterDaoImpl();
			ItemManufacturingMaster manufact = itemManufacturingMasterDao.selectById(1, itemId, date);

			task.execute(manufact);
		});
	}
}
