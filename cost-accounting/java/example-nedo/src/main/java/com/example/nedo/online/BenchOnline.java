package com.example.nedo.online;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.InitialData;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;

public class BenchOnline {

	public static void main(String[] args) {
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
						create1(threadList, start, end, batchDate);
					} else {
						int id = Integer.parseInt(s.trim());
						if (!set.add(id)) {
							throw new IllegalArgumentException("duplicate id=" + id);
						}
						create1(threadList, id, id, batchDate);
					}
				}
			}
		}
		if (threadList.isEmpty()) {
			List<Integer> factoryList = getAllFactory();
			int start = factoryList.stream().mapToInt(n -> n).min().getAsInt();
			int end = factoryList.stream().mapToInt(n -> n).max().getAsInt();
			create1(threadList, start, end, batchDate);
		}

		ExecutorService pool = newExecutorService(threadList.size());

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
				System.err.println("----");
				e.printStackTrace();
			}
		}

		// TODO 処理件数カウンター（バッチの実行中のものだけカウントする、なんて出来るのか？）
	}

	private static List<Integer> getAllFactory() {
		FactoryMasterDao dao = new FactoryMasterDaoImpl();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		return tm.required(() -> {
			return dao.selectAllId();
		});
	}

	private static void create1(List<BenchOnlineThread> threadList, int start, int end, LocalDate date) {
		System.out.printf("create thread: factoryId=%d-%d%n", start, end);
		List<Integer> factoryList = IntStream.rangeClosed(start, end).boxed().collect(Collectors.toList());
		BenchOnlineThread thread = new BenchOnlineThread(factoryList, date);
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
