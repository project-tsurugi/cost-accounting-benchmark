package com.example.nedo.online.task;

import java.time.LocalDate;
import java.time.LocalDateTime;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.init.BenchRandom;
import com.example.nedo.jdbc.doma2.dao.CostMasterDao;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;

public abstract class BenchOnlineTask {

	private final String title;

	protected TransactionManager tm;
	protected ItemManufacturingMasterDao itemManufacturingMasterDao;
	protected FactoryMasterDao factoryMasterDao;
	protected ItemMasterDao itemMasterDao;
	protected ItemConstructionMasterDao itemCostructionMasterDao;
	protected CostMasterDao costMasterDao;
	protected ResultTableDao resultTableDao;

	protected int factoryId;
	protected LocalDate date;

	protected final BenchRandom random = new BenchRandom();

	public BenchOnlineTask(String title) {
		this.title = title;
	}

	public void setDao(TransactionManager tm, ItemManufacturingMasterDao itemManufacturingMasterDao,
			FactoryMasterDao factoryMasterDao, ItemMasterDao itemMasterDao,
			ItemConstructionMasterDao itemCostructionMasterDao, CostMasterDao costMasterDao,
			ResultTableDao resultTableDao) {
		this.tm = tm;
		this.itemManufacturingMasterDao = itemManufacturingMasterDao;
		this.factoryMasterDao = factoryMasterDao;
		this.itemMasterDao = itemMasterDao;
		this.itemCostructionMasterDao = itemCostructionMasterDao;
		this.costMasterDao = costMasterDao;
		this.resultTableDao = resultTableDao;
	}

	public void initialize(int factoryId, LocalDate date) {
		this.factoryId = factoryId;
		this.date = date;
	}

	public abstract void execute();

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
}
