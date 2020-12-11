package com.example.nedo.batch;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentLinkedDeque;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.BenchRandom;
import com.example.nedo.init.InitialData;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.CostMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDaoImpl;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDaoImpl;
import com.example.nedo.jdbc.doma2.entity.ItemManufacturingMaster;

public class BenchBatch {

	private final BenchRandom random = new BenchRandom();
	private int commitRatio;

	public static void main(String[] args) {
		LocalDate batchDate = InitialData.DEFAULT_BATCH_DATE;
		if (args.length >= 1) {
			batchDate = LocalDate.parse(args[0]);
		}

		List<Integer> factoryList = new ArrayList<>();
		if (args.length >= 2) {
			String[] ss = args[1].split(",");
			for (String s : ss) {
				factoryList.add(Integer.parseInt(s.trim()));
			}
		}

		int commitRatio = 100;
		if (args.length >= 3) {
			commitRatio = Integer.parseInt(args[2].trim());
		}

		new BenchBatch().main(batchDate, factoryList, commitRatio);
	}

	public void main(LocalDate batchDate, List<Integer> factoryList, int commitRatio) {
		logStart();

		assert batchDate != null;
		if (factoryList == null || factoryList.isEmpty()) {
			factoryList = getAllFactory();
		}
		this.commitRatio = commitRatio;

		System.out.println("batchDate=" + batchDate);
		System.out.println("factory=" + factoryList);
		System.out.println("commitRatio=" + commitRatio);

		switch (2) {
		case 1:
			executeSequential(batchDate, factoryList);
			break;
		case 2:
			executeParallelFactory(batchDate, factoryList);
			break;
		case 3:
			executeStream(batchDate, factoryList);
			break;
		case 4:
			executeQueue(batchDate, factoryList);
			break;
		case 5:
			executeForDebug1(batchDate);
			break;
		}

		logEnd();
	}

	private LocalDateTime startTime;

	protected void logStart() {
		startTime = LocalDateTime.now();
		System.out.println("start " + startTime);
	}

	protected void logEnd() {
		LocalDateTime endTime = LocalDateTime.now();
		System.out.println("end " + startTime.until(endTime, ChronoUnit.SECONDS));
	}

	private List<Integer> getAllFactory() {
		FactoryMasterDao dao = new FactoryMasterDaoImpl();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		return tm.required(() -> {
			return dao.selectAllId();
		});
	}

	private void executeSequential(LocalDate batchDate, List<Integer> factoryList) {
		for (int factoryId : factoryList) {
			BenchBatchFactoryThread thread = new BenchBatchFactoryThread(this, batchDate, factoryId);

			thread.run();
		}
	}

	private void executeParallelFactory(LocalDate batchDate, List<Integer> factoryList) {
		List<BenchBatchFactoryThread> threadList = factoryList.stream()
				.map(factoryId -> new BenchBatchFactoryThread(this, batchDate, factoryId)).collect(Collectors.toList());

		ExecutorService pool = Executors.newFixedThreadPool(factoryList.size());
		List<Future<Void>> resultList = Collections.emptyList();
		try {
			resultList = pool.invokeAll(threadList);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			pool.shutdownNow();
		}
		for (Future<Void> result : resultList) {
			try {
				result.get(); // 例外が発生していた場合にそれを取り出す
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	private void executeStream(LocalDate batchDate, List<Integer> factoryList) {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		tm.required(() -> {
			ResultTableDao resultTableDao = new ResultTableDaoImpl();
			resultTableDao.deleteByFactories(factoryList, batchDate);

			BenchBatchItemTask itemTask = new BenchBatchItemTask(batchDate);
			itemTask.setDao(new ItemConstructionMasterDaoImpl(), new ItemMasterDaoImpl(), new CostMasterDaoImpl(),
					resultTableDao);

			ItemManufacturingMasterDao itemManufacturingMasterDao = new ItemManufacturingMasterDaoImpl();
			try (Stream<ItemManufacturingMaster> stream0 = itemManufacturingMasterDao.selectByFactories(factoryList,
					batchDate)) {
				Stream<ItemManufacturingMaster> stream = stream0;
//				Stream<MakeItemMaster> stream = stream0.parallel(); // 効果が無いっぽい
				stream.parallel().forEach(manufact -> {
					itemTask.execute(manufact);
				});
			}

			commitOrRollback(tm, random);
		});
	}

	private void executeQueue(LocalDate batchDate, List<Integer> factoryList) {
		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		ResultTableDao resultTableDao = new ResultTableDaoImpl();

		ConcurrentLinkedDeque<ItemManufacturingMaster> queue = tm.required(() -> {
			ConcurrentLinkedDeque<ItemManufacturingMaster> q;
			resultTableDao.deleteByFactories(factoryList, batchDate);

			ItemManufacturingMasterDao itemManufacturingMasterDao = new ItemManufacturingMasterDaoImpl();
			try (Stream<ItemManufacturingMaster> stream = itemManufacturingMasterDao.selectByFactories(factoryList,
					batchDate)) {
				q = stream.collect(Collectors.toCollection(ConcurrentLinkedDeque::new));
			}
			return q;
		});

		BenchBatchItemTask itemTask = new BenchBatchItemTask(batchDate);
		itemTask.setDao(new ItemConstructionMasterDaoImpl(), new ItemMasterDaoImpl(), new CostMasterDaoImpl(),
				resultTableDao);

		int size = 8;
		ExecutorService pool = Executors.newFixedThreadPool(size);
		List<Callable<Void>> list = Stream.generate(() -> new Callable<Void>() {

			@Override
			public Void call() throws Exception {
				TransactionManager tm = AppConfig.singleton().getTransactionManager();
				tm.required(() -> {
					for (;;) {
						ItemManufacturingMaster manufact = queue.pollLast();
						if (manufact == null) {
							break;
						}
						itemTask.execute(manufact);
					}

					commitOrRollback(tm, random);
				});
				return null;
			}
		}).limit(size).collect(Collectors.toList());

		try {
			pool.invokeAll(list);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			pool.shutdown();
		}
	}

	private void executeForDebug1(LocalDate batchDate) {
		int factoryId = 1;
		int productId = 345859;

		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		ResultTableDao resultTableDao = new ResultTableDaoImpl();
		ItemManufacturingMasterDao itemManufacturingMasterDao = new ItemManufacturingMasterDaoImpl();

		BenchBatchItemTask itemTask = new BenchBatchItemTask(batchDate);
		itemTask.setDao(new ItemConstructionMasterDaoImpl(), new ItemMasterDaoImpl(), new CostMasterDaoImpl(),
				resultTableDao);

		tm.required(() -> {
			ItemManufacturingMaster manufact = itemManufacturingMasterDao.selectById(factoryId, productId, batchDate);

			resultTableDao.deleteByProductId(factoryId, batchDate, productId);

			itemTask.execute(manufact);
		});
	}

	public void commitOrRollback(TransactionManager tm, BenchRandom random) {
		int n = random.random(0, 99);
		if (n < commitRatio) {
			// commit
		} else {
			// rollback
			tm.setRollbackOnly();
		}
	}
}
