package com.example.nedo.online;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.example.nedo.BenchConst;
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

		List<BenchOnlineThread> threadList = new ArrayList<>();
		if (args.length >= 2) {
			if (!args[1].trim().equalsIgnoreCase("all")) {
				Set<Integer> set = new HashSet<>();
				String[] ss = args[1].split(",");
				for (String s : ss) {
					int n = s.indexOf('-');
					if (n >= 0) {
						int start = Integer.parseInt(s.substring(0, n).trim());
						int end = Integer.parseInt(s.substring(n + 1).trim());
						for (int id = start; id <= end; id++) {
							if (!set.add(id)) {
								throw new IllegalArgumentException("duplicate id=" + id);
							}
						}
						create1(manager, threadList, start, end, batchDate);
					} else {
						int id = Integer.parseInt(s.trim());
						if (!set.add(id)) {
							throw new IllegalArgumentException("duplicate id=" + id);
						}
						create1(manager, threadList, id, id, batchDate);
					}
				}
			}
		}
		if (threadList.isEmpty()) {
			List<Integer> factoryList = getAllFactory(manager);
			int start = factoryList.stream().mapToInt(n -> n).min().getAsInt();
			int end = factoryList.stream().mapToInt(n -> n).max().getAsInt();
			create1(manager, threadList, start, end, batchDate);
		}

		ExecutorService pool = newExecutorService(threadList.size());
		AtomicBoolean done = new AtomicBoolean(false);
		try {
			Runtime.getRuntime().addShutdownHook(new Thread() {
				@Override
				public void run() {
					System.out.println("shutdown-hook start");
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

			List<Future<Void>> resultList = new ArrayList<>(threadList.size());
			for (BenchOnlineThread thread : threadList) {
				Future<Void> future = pool.submit((Callable<Void>) thread);
				resultList.add(future);
			}

			// 終了待ち
			for (Future<Void> result : resultList) {
				try {
					result.get();
				} catch (Exception ignore) {
					// ignore
				}
			}

			// 例外出力
			for (Future<Void> result : resultList) {
				try {
					result.get(); // 例外が発生していた場合にそれを取り出す
				} catch (CancellationException ignore) {
					// ignore
				} catch (Exception e) {
					System.err.println("----");
					e.printStackTrace();
				}
			}
		} finally {
			try {
				pool.shutdownNow();
			} finally {
				done.set(true);
			}
		}

		// TODO 処理件数カウンター（バッチの実行中のものだけカウントする、なんて出来るのか？）
	}

	private static List<Integer> getAllFactory(CostBenchDbManager manager) {
		FactoryMasterDao dao = manager.getFactoryMasterDao();

		return manager.execute(() -> {
			return dao.selectAllId();
		});
	}

	private static void create1(CostBenchDbManager manager, List<BenchOnlineThread> threadList, int start, int end,
			LocalDate date) {
		System.out.printf("create thread: factoryId=%d-%d%n", start, end);
		List<Integer> factoryList = IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
		BenchOnlineThread thread = new BenchOnlineThread(manager, factoryList, date);
		threadList.add(thread);
	}

	static ExecutorService newExecutorService(int size) {
		// return Executors.newFixedThreadPool(size);
		return new ThreadPoolExecutor(size, size, 0L, TimeUnit.MILLISECONDS, new LinkedBlockingQueue<Runnable>()) {

			@Override
			protected void afterExecute(Runnable r, Throwable t) {
				Future<?> task = (Future<?>) r;
				try {
					task.get();
				} catch (InterruptedException e) {
					e.printStackTrace();
				} catch (ExecutionException e) {
					shutdownNow();
				}
			}
		};
	}
}
