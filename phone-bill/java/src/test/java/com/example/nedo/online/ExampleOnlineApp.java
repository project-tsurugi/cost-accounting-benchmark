package com.example.nedo.online;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * AbstractOnlineAppの動作確認用サンプルアプリ
 *
 */
public class ExampleOnlineApp extends AbstractOnlineApp {
    private static final Logger LOG = LoggerFactory.getLogger(ExampleOnlineApp.class);
	private static final DateFormat DF = new SimpleDateFormat("HH:mm:ss.SSS");

	public ExampleOnlineApp() {
		super(20, new Random());
	}

	@Override
	void exec() {
		LOG.info("executed.");
	}

	public static void main(String[] args) throws InterruptedException {
		ExampleOnlineApp app = new ExampleOnlineApp();
		ExecutorService service = Executors.newSingleThreadExecutor();
		service.submit(app);
		Thread.sleep(60 * 1000 * 5); // 5分実行して終了する。
		app.terminate();
		service.shutdown();
		service.awaitTermination(1, TimeUnit.DAYS);
	}

	@Override
	protected void atScheduleListCreated(List<Long> scheduleList) {
		for(long schedule: scheduleList) {
			LOG.info("Scheduled at {}", DF.format(new Date(schedule)));
		}
	}

	@Override
	protected void cleanup() {
		LOG.info("Cleanup called");
		throw new RuntimeException(); // cleanup()でExceptionが起きたときの動作を見る
	}
}
