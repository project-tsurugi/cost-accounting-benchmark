package com.example.nedo.jdbc.raw;

import java.sql.Connection;
import java.util.function.Supplier;

import com.example.nedo.jdbc.CostBenchDbManager;
import com.example.nedo.jdbc.doma2.dao.CostMasterDao;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemConstructionMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemManufacturingMasterDao;
import com.example.nedo.jdbc.doma2.dao.ItemMasterDao;
import com.example.nedo.jdbc.doma2.dao.MeasurementMasterDao;
import com.example.nedo.jdbc.doma2.dao.ResultTableDao;
import com.example.nedo.jdbc.raw.dao.CostMasterDaoRaw;
import com.example.nedo.jdbc.raw.dao.FactoryMasterDaoRaw;
import com.example.nedo.jdbc.raw.dao.ItemConstructionMasterDaoRaw;
import com.example.nedo.jdbc.raw.dao.ItemManufacturingMasterDaoRaw;
import com.example.nedo.jdbc.raw.dao.ItemMasterDaoRaw;
import com.example.nedo.jdbc.raw.dao.MeasurementMasterDaoRaw;
import com.example.nedo.jdbc.raw.dao.ResultTableDaoRaw;

public abstract class CostBenchDbManagerJdbc extends CostBenchDbManager {

	public CostBenchDbManagerJdbc() {
	}

	public abstract Connection getConnection();

	@Override
	protected MeasurementMasterDao newMeasurementMasterDao() {
		return new MeasurementMasterDaoRaw(this);
	}

	@Override
	protected FactoryMasterDao newFactoryMasterDao() {
		return new FactoryMasterDaoRaw(this);
	}

	@Override
	protected ItemMasterDao newItemMasterDao() {
		return new ItemMasterDaoRaw(this);
	}

	@Override
	protected ItemConstructionMasterDao newItemConstructionMasterDao() {
		return new ItemConstructionMasterDaoRaw(this);
	}

	@Override
	protected ItemManufacturingMasterDao newItemManufacturingMasterDao() {
		return new ItemManufacturingMasterDaoRaw(this);
	}

	@Override
	protected CostMasterDao newCostMasterDao() {
		return new CostMasterDaoRaw(this);
	}

	@Override
	protected ResultTableDao newResultTableDao() {
		return new ResultTableDaoRaw(this);
	}

	@Override
	public void execute(Runnable runnable) {
		try {
			runnable.run();
			commit();
		} catch (Throwable e) {
			try {
				rollback();
			} catch (Throwable t) {
				e.addSuppressed(t);
			}
			throw e;
		}
	}

	@Override
	public <T> T execute(Supplier<T> supplier) {
		try {
			T r = supplier.get();
			commit();
			return r;
		} catch (Throwable e) {
			try {
				rollback();
			} catch (Throwable t) {
				e.addSuppressed(t);
			}
			throw e;
		}
	}
}
