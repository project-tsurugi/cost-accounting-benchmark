package com.example.nedo.online;

import java.time.LocalDate;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import com.example.nedo.init.InitialData;

public class BenchOnline {

	public static void main(String[] args) {
		List<BenchOnlineThread> threadList = new ArrayList<>();
		// TODO argsから工場IDと日付を取得
		create1(threadList, 1, 2, InitialData.DEFAULT_BATCH_DATE);
		create1(threadList, 3, 4, InitialData.DEFAULT_BATCH_DATE);
		create1(threadList, 5, 6, InitialData.DEFAULT_BATCH_DATE);

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

	private static void create1(List<BenchOnlineThread> threadList, int start, int end, LocalDate date) {
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
