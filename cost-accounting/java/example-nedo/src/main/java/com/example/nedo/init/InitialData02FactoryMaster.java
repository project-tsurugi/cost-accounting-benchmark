package com.example.nedo.init;

import org.seasar.doma.jdbc.tx.TransactionManager;

import com.example.nedo.BenchConst;
import com.example.nedo.jdbc.doma2.config.AppConfig;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDao;
import com.example.nedo.jdbc.doma2.dao.FactoryMasterDaoImpl;
import com.example.nedo.jdbc.doma2.entity.FactoryMaster;

public class InitialData02FactoryMaster extends InitialData {

	public static void main(String[] args) throws Exception {
		int factorySize = BenchConst.initFactorySize();
		new InitialData02FactoryMaster().main(factorySize);
	}

	public InitialData02FactoryMaster() {
		super(null);
	}

	private void main(int size) {
		logStart();

		generateFactoryMaster(size);

		logEnd();
	}

	private void generateFactoryMaster(int size) {
		FactoryMasterDao dao = new FactoryMasterDaoImpl();

		TransactionManager tm = AppConfig.singleton().getTransactionManager();

		tm.required(() -> {
			dao.deleteAll();
			insertFactoryMaster(size, dao);
		});
	}

	private void insertFactoryMaster(int size, FactoryMasterDao dao) {
		for (int i = 0; i < size; i++) {
			int fId = i + 1;

			FactoryMaster entity = new FactoryMaster();
			entity.setFId(fId);
			entity.setFName("Factory" + fId);

			dao.insert(entity);
		}
	}
}
