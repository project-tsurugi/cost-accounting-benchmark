package com.example.nedo.online.task;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.TimeUnit;

import com.example.nedo.BenchConst;
import com.example.nedo.init.BenchRandom;
import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.online.BenchOnline;

public abstract class BenchOnlineTask {

	private final String title;

	protected CostBenchDbManager dbManager;

	protected int factoryId;
	protected LocalDate date;

	protected final BenchRandom random = new BenchRandom();

	public BenchOnlineTask(String title) {
		this.title = title;
	}

	public String getTitle() {
		return title;
	}

	public void setDao(CostBenchDbManager dbManager) {
		this.dbManager = dbManager;
	}

	public void initialize(int factoryId, LocalDate date) {
		this.factoryId = factoryId;
		this.date = date;
	}

	public final void execute() {
		logStart("factory=%d, date=%s", factoryId, date);

		execute1();

		logEnd("factory=%d, date=%s", factoryId, date);
	}

	protected abstract void execute1();

	protected void logStart(String format, Object... args) {
		String s = String.format(format, args);
		logTime("start ", s);
	}

	protected void logTarget(String format, Object... args) {
		String s = String.format(format, args);
		logTime("target", s);
	}

	protected void logEnd(String format, Object... args) {
		String s = String.format(format, args);
		logTime("end   ", s);
	}

	protected void logTime(String sub, String message) {
		String s = title + " " + sub + " " + LocalDateTime.now() + ": " + message;
		log0(s);
	}

	protected void log(String message) {
		String s = title + ": " + message;
		log0(s);
	}

	protected void log0(String message) {
		System.out.println(message); // TODO
	}

	protected void console(String format, Object... args) {
		String s = String.format(format, args);
		System.out.println(s);
	}

	private long sleepTime = Integer.MIN_VALUE;

	public long getSleepTime() {
		if (sleepTime < 0) {
			this.sleepTime = TimeUnit.SECONDS.toMillis(BenchConst.onlineTaskSleepTime(title));
		}
		return sleepTime;
	}

	protected static final CostBenchDbManager createCostBenchDbManagerForTest() {
		return BenchOnline.createCostBenchDbManager();
	}
}
