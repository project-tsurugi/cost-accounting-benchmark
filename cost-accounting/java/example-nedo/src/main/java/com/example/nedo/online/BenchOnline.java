package com.example.nedo.online;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import com.example.nedo.BenchConst;
import com.example.nedo.batch.StringUtil;
import com.example.nedo.init.InitialData;
import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;

public class BenchOnline {

	public static void main(String[] args) {
		try (CostBenchDbManager manager = createCostBenchDbManager()) {
			main0(args, manager);
		}
	}

	public static CostBenchDbManager createCostBenchDbManager() {
		int type = BenchConst.onlineJdbcType();
		return CostBenchDbManager.createInstance(type);
	}

	private static void main0(String[] args, CostBenchDbManager manager) {
		LocalDate batchDate = InitialData.DEFAULT_BATCH_DATE;
		if (args.length >= 1) {
			batchDate = LocalDate.parse(args[0]);
		}

		List<BenchOnlineThread> threadList = createThread((args.length >= 2) ? args[1] : "all", manager, batchDate);

		ExExecutorService pool = newExecutorService(threadList.size());
		AtomicBoolean done = new AtomicBoolean(false);
		try {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
//					System.out.println("shutdown-hook start");
					pool.shutdownNow();

					// 終了待ち
					while (!done.get()) {
						try {
							Thread.sleep(100);
						} catch (InterruptedException ignore) {
							// ignore
						}
					}
				}
			});

			pool.invokeAll(threadList);
		} catch (InterruptedException e) {
			e.printStackTrace();
		} finally {
			try {
				pool.shutdownNow();

				// 例外出力
				pool.printException();
			} finally {
				done.set(true);
			}
		}
	}

	private static List<BenchOnlineThread> createThread(String arg, CostBenchDbManager manager, LocalDate batchDate) {
		List<BenchOnlineThread> threadList = new ArrayList<>();

		if (!arg.trim().equalsIgnoreCase("all")) {
			Set<Integer> set = new HashSet<>();
			String[] ss = arg.split(";");
			for (String s : ss) {
				List<Integer> list = convertId(s);
				for (Integer id : list) {
					if (!set.add(id)) {
						throw new IllegalArgumentException("duplicate id=" + id);
					}
				}
				create1(manager, threadList, list, batchDate);
			}
		}

		if (threadList.isEmpty()) {
			List<Integer> factoryList = getAllFactory(manager);
			for (Integer id : factoryList) {
				create1(manager, threadList, Collections.singletonList(id), batchDate);
			}
		}

		return threadList;
	}

	private static List<Integer> convertId(String arg) {
		Set<Integer> set = new TreeSet<>();

		String[] ss = arg.split(",");
		for (String s : ss) {
			int n = s.indexOf('-');
			if (n >= 0) {
				int start = Integer.parseInt(s.substring(0, n).trim());
				int end = Integer.parseInt(s.substring(n + 1).trim());
				for (int id = start; id <= end; id++) {
					set.add(id);
				}
			} else {
				int id = Integer.parseInt(s.trim());
				set.add(id);
			}
		}

		return new ArrayList<>(set);
	}

	private static List<Integer> getAllFactory(CostBenchDbManager manager) {
		FactoryMasterDao dao = manager.getFactoryMasterDao();

		return manager.execute(() -> {
			return dao.selectAllId();
		});
	}

	private static int threadId = 0;

	private static void create1(CostBenchDbManager manager, List<BenchOnlineThread> threadList, List<Integer> idList,
			LocalDate date) {
		System.out.printf("create thread%d: factoryId=%s%n", threadId, StringUtil.toString(idList));
		BenchOnlineThread thread = new BenchOnlineThread(threadId++, manager, idList, date);
		threadList.add(thread);
	}

	static ExExecutorService newExecutorService(int size) {
		// return Executors.newFixedThreadPool(size);
		return new ExExecutorService(size);
	}

	private static class ExExecutorService extends ThreadPoolExecutor {
		private final List<Exception> exceptionList = new CopyOnWriteArrayList<>();

		public ExExecutorService(int nThreads) {
			super(nThreads, nThreads, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>());
		}

		@Override
		protected void afterExecute(Runnable r, Throwable t) {
			Future<?> task = (Future<?>) r;
			try {
				task.get();
			} catch (InterruptedException | CancellationException ignore) {
				// ignore
			} catch (Exception e) {
				exceptionList.add(e);
				shutdownNow();
			}
		}

		public void printException() {
			for (Exception e : exceptionList) {
				System.err.println("----");
				e.printStackTrace();
			}
		}
	}
}
